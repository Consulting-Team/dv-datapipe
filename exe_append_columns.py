"""
이미 입력된 raw 테이블에 열 추가
"""

import sys
from config import config, logger
from utils import metadata
from utils import data_loader


def check_columns(metadata_dict: dict[str, list[metadata.MetaData]]):
    """
    메타데이터 파일에 정의된 태그와 iceberg 테이블 열 비교
    - 사용자가 확인하고 직접 열 추가 명령을 내리도록 함 --> 무분별한 열 추가 방지
    - 추가해야 할 열이 발견되면 열 추가 SQL 출력 후 프로그램 종료
    - 사용자가 SQL 실행 후 다시 프로그램 실행
    """
    logger.info(f"👉 Checking table columns.")
    hnum = config.hull
    suffix = "raw"
    db_name = "dv"
    table_name = f"""{hnum.lower()}_{suffix}"""
    metadata_map = {
        md.col_name: md.data_type
        for md_list in metadata_dict.values()
        for md in md_list
    }
    # SRC 테이블에 대한 id 추가
    for table in metadata_dict:
        metadata_map[f"id_{table}"] = "BIGINT"
    metadata_cols = set(metadata_map.keys())

    # raw 테이블 열 불러오기
    existing_df = data_loader.get_columns(
        db_name=db_name, table_name=f"{hnum}_{suffix}"
    )
    existing_cols = set(existing_df["name"])

    # 신규 컬럼
    new_cols = [f"{c}" for c in metadata_cols if c not in existing_cols]

    if new_cols:
        max_len = max(len(c) for c in new_cols)
        col_definitions = [f"{c.ljust(max_len)} {metadata_map[c]}" for c in new_cols]

        sql = f"ALTER TABLE {db_name}.{table_name} ADD COLUMNS (\n\t{',\n\t'.join(col_definitions)}\n);"

        logger.info(
            f"   - New columns detected. Please update the schema of '{db_name}.{table_name}'."
        )
        logger.info(f"=" * 100)
        logger.info(f"\n{sql}")
        logger.info(f"=" * 100)

        sys.exit(1)

    return sorted(list(metadata_cols))


def main():
    config.display()
    start = config.start.strftime("%Y%m%d")
    end = config.end.strftime("%Y%m%d")

    # 추가할 열 메타데이터 읽기
    metadata_path = config.metapath
    metadata_dict = metadata.read_metadata(metadata_path)

    # check column
    # 메타데이터와 raw 테이블 열 비교 -> 추가해야 할 열 확인
    new_cols = check_columns(metadata_dict=metadata_dict)

    # 데이터 삽입
    df = data_loader.query_data(metadata_dict, start, end)
    # cols = df에서 추출 --> ["col_name    data_type"]


    return


if __name__ == "__main__":
    main()
