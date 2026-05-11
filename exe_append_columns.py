"""
이미 입력된 raw 테이블에 열 추가
"""

import sys
import inspect
from config import config, logger
from utils import metadata
from utils import data_loader, schema_manager as sm
from connection import ImpalaConnection


def check_columns(
    metadata_dict: dict[str, list[metadata.MetaData]],
) -> list[str] | None:
    """
    메타데이터 파일에 정의된 태그와 iceberg 테이블 열 비교
    """
    logger.info(f"👉 Checking table columns.")
    hnum = config.hull
    suffix = "raw"
    db_name = "dv"
    # table_name = f"""{hnum.lower()}_{suffix}"""
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

        # sql = f"ALTER TABLE {db_name}.{table_name} ADD COLUMNS (\n\t{',\n\t'.join(col_definitions)}\n);"

        # logger.info(
        #     f"   - New columns detected. Please update the schema of '{db_name}.{table_name}'."
        # )

        logger.info(f"   - New columns were detected.")

        return col_definitions

        # logger.info(f"=" * 100)
        # logger.info(f"\n{sql}")
        # logger.info(f"=" * 100)

        # conn = ImpalaConnection().conn

        # try:
        #     cursor = conn.cursor()
        #     cursor.execute(sql)
        #     logger.info("   - New columns were appended.")
        # except Exception as e:
        #     logger.error(f"❌ {e} [{__file__}:{inspect.currentframe().f_lineno}]")
        #     raise e

        # sys.exit(1)

    return None


def append_columns(cols: list[str]):
    """
    TARGET 테이블에 열 추가
    """
    logger.info(f"👉 Appendding new columns.")

    conn = ImpalaConnection().conn
    hnum = config.hull
    suffix = "raw"
    db_name = "dv"
    table_name = f"""{hnum.lower()}_{suffix}"""
    sql = (
        f"ALTER TABLE {db_name}.{table_name} ADD COLUMNS (\n\t{',\n\t'.join(cols)}\n);"
    )

    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        logger.debug(f"\n{sql}")
        logger.info(
            f"   - {len(cols)} {'column was' if len(cols) == 1 else 'columns were'} appended."
        )
    except Exception as e:
        logger.error(f"❌ {e} [{__file__}:{inspect.currentframe().f_lineno}]")
        raise e

    return


def main():
    config.display()
    start = config.start.strftime("%Y%m%d")
    end = config.end.strftime("%Y%m%d")

    # 추가할 열 메타데이터 읽기
    metadata_path = config.metapath
    metadata_dict = metadata.read_metadata(metadata_path)
    hulls_in_metadata = {table.split("_")[0].lower() for table in metadata_dict.keys()}

    # 호선 번호 체크
    if config.hull.lower() not in hulls_in_metadata or len(hulls_in_metadata) > 1:
        print(f"Warning: Hull number mismatch!")
        print(f" - Config: {config.hull}")
        print(f" - Metadata found: {', '.join(hulls_in_metadata)}")

        confirm = input("Will you proceed the process? [y/n]: ").lower()
    if confirm != "y":
        print("Process aborted.")
        exit()

    col_names = ["ds_timestamp    TIMESTAMP"]
    col_names += [
        f"{md.col_name}    {md.data_type}"
        for md_list in metadata_dict.values()
        for md in md_list
    ]
    col_names.extend([f"id_{table_name}    BIGINT" for table_name in metadata_dict.keys()])

    # check column
    # 메타데이터와 raw 테이블 열 비교 -> 추가해야 할 열 확인
    new_cols = check_columns(metadata_dict=metadata_dict)

    if new_cols:
        append_columns(new_cols)

    # 데이터 삽입
    # df = data_loader.query_data(metadata_dict, start, end)
    sm.insert_data(col_names, metadata_dict, start=start, end=end, add_cols=False)
    # cols = df에서 추출 --> ["col_name    data_type"]
    # cols = df.columns

    return


if __name__ == "__main__":
    main()
