"""과제 01 — 부동산 실거래가 Bronze (API 수집)

국토교통부 아파트 매매 실거래가 API 를 시군구 6곳 병렬로 호출해 응답 XML 을 가공 없이
s3://realestate-{이름}/bronze/{yyyymm}/{LAWD_CD}.xml 로 저장한다.

  collect_group (TaskGroup, 6 tasks) → branch_after_collect ─┬→ summary_done  (전부 정상)
                                                              └→ skip_upload   (0건/파싱 실패 존재)

Airflow core + 기본 이미지 패키지(requests, boto3)만 사용 → 과제 04 Helm Airflow 에서도 import 에러 없이 로드된다.
"""
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from urllib.parse import unquote

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup

API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
LAWD_CODES = ["11680", "11650", "11710", "11440", "11170", "11200"]  # 강남·서초·송파·마포·용산·성동
NUM_OF_ROWS = 1000  # API 기본값은 10건 → 크게 잡고 totalCount 기준으로 페이지 순회

COLLECTOR_NAME = os.environ.get("COLLECTOR_NAME", "한승수")
BUCKET = os.environ.get("REALESTATE_BUCKET", "realestate-seungsoohan")
SERVICE_KEY = os.environ.get("DATA_GO_KR_SERVICE_KEY", "")  # 제출 시 빈 문자열 — docker-compose/.env 에서 주입


def _fetch_page(lawd_cd, deal_ymd, page_no):
    import requests

    params = {
        # 포털의 Encoding 키를 넣어도 이중 인코딩되지 않도록 한 번 디코딩
        "serviceKey": unquote(SERVICE_KEY),
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "pageNo": page_no,
        "numOfRows": NUM_OF_ROWS,
    }
    response = requests.get(API_URL, params=params, timeout=30)
    if response.ok:
        return response.content
    # requests 기본 예외 메시지는 serviceKey 가 포함된 URL 을 그대로 찍으므로 키를 뺀 메시지로 다시 던진다
    detail = f"HTTP {response.status_code} (LAWD_CD={lawd_cd}, DEAL_YMD={deal_ymd}, pageNo={page_no})"
    if response.status_code in (401, 403):
        from airflow.exceptions import AirflowFailException

        raise AirflowFailException(f"{detail}: 인증 실패 — DATA_GO_KR_SERVICE_KEY 확인 (재시도 안 함)")
    raise RuntimeError(f"{detail}: API 호출 실패 — 재시도")


def _parse(raw):
    """(root, result_code, total_count, items). 파싱 실패 시 ET.ParseError."""
    root = ET.fromstring(raw)
    # 정상: <response><header><resultCode>000 / 인증 오류 등: <OpenAPI_ServiceResponse>...<returnReasonCode>
    result_code = root.findtext(".//resultCode") or root.findtext(".//returnReasonCode") or ""
    total_count = int(root.findtext(".//totalCount") or 0)
    return root, result_code.strip(), total_count, root.findall(".//items/item")


def collect(lawd_cd, data_interval_start, **_):
    print(f'collector={COLLECTOR_NAME}, time={datetime.now()}, lawd={lawd_cd}')

    deal_ymd = data_interval_start.in_timezone("Asia/Seoul").strftime("%Y%m")
    key = f"bronze/{deal_ymd}/{lawd_cd}.xml"
    result = {"lawd_cd": lawd_cd, "deal_ymd": deal_ymd, "s3_key": key, "count": 0}

    raw = _fetch_page(lawd_cd, deal_ymd, 1)
    try:
        root, result_code, total_count, items = _parse(raw)
    except ET.ParseError as error:
        print(f"[parse_error] {lawd_cd} {deal_ymd}: {error} / head={raw[:200]!r}")
        return {**result, "status": "parse_error"}

    if result_code != "000":
        print(f"[api_error] {lawd_cd} {deal_ymd}: resultCode={result_code} / {raw[:300]!r}")
        return {**result, "status": "api_error"}

    # totalCount 가 한 페이지를 넘으면 나머지 페이지의 <item> 을 첫 응답 문서에 이어 붙인다
    pages = -(-total_count // NUM_OF_ROWS)
    if pages > 1:
        items_node = root.find(".//items")
        for page_no in range(2, pages + 1):
            _, _, _, more = _parse(_fetch_page(lawd_cd, deal_ymd, page_no))
            items_node.extend(more)
            items.extend(more)
        raw = ET.tostring(root, encoding="utf-8", xml_declaration=True)

    print(f"{lawd_cd} {deal_ymd}: totalCount={total_count}, items={len(items)}, pages={max(pages, 1)}")
    if not items:
        print(f"[empty] {lawd_cd} {deal_ymd}: 응답 0건 — S3 업로드 생략")
        return {**result, "status": "empty"}

    import boto3

    boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")).put_object(
        Bucket=BUCKET, Key=key, Body=raw, ContentType="application/xml"
    )
    print(f"uploaded s3://{BUCKET}/{key} ({len(raw):,} bytes)")
    return {**result, "status": "uploaded", "count": len(items)}


def branch_after_collect(ti, **_):
    results = ti.xcom_pull(task_ids=[f"collect_group.collect_{code}" for code in LAWD_CODES])
    for r in results:
        print(f"  {r['lawd_cd']} {r['deal_ymd']}: {r['status']:<11} {r['count']:>5}건  s3://{BUCKET}/{r['s3_key']}")
    failed = [r["lawd_cd"] for r in results if r["status"] != "uploaded"]
    if failed:
        print(f"0건/파싱 실패 시군구: {failed} → skip_upload")
        return "skip_upload"
    print(f"6개 시군구 모두 업로드 완료 (총 {sum(r['count'] for r in results):,}건) → summary_done")
    return "summary_done"


with DAG(
    dag_id="bronze_realestate_collect",
    description="과제01 국토부 아파트 매매 실거래가 API → S3 bronze (raw XML)",
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    end_date=pendulum.datetime(2024, 3, 31, tz="Asia/Seoul"),  # 2024년 1분기(1~3월) 백필
    catchup=True,
    max_active_runs=1,
    default_args={"owner": COLLECTOR_NAME, "retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["bronze", "realestate", "q1"],
) as dag:
    with TaskGroup("collect_group") as collect_group:
        for code in LAWD_CODES:
            PythonOperator(task_id=f"collect_{code}", python_callable=collect, op_kwargs={"lawd_cd": code})

    branch = BranchPythonOperator(task_id="branch_after_collect", python_callable=branch_after_collect)
    skip_upload = EmptyOperator(task_id="skip_upload")
    summary_done = EmptyOperator(task_id="summary_done")

    collect_group >> branch >> [skip_upload, summary_done]
