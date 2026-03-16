#! H2521호선 iceberg 테이블 생성 테스트

from config import config
from jmlogger import logger
from connection import ImpalaConnection, abfs
from utils.metadata import read_metadata, MetaData, get_db_name
from functools import reduce
from pathlib import Path
import polars as pl

# 테이블이 생성될 DB
DB_NAME = "tmp"
# 새로 생성할 테이블 이름
TABLE_NAME = f"{config.hull}_iceberg_test"


def get_storage_location(hull=None) -> str:
    # 클라우드 테이블 저장 위치 반환
    container_name = config.hs4v1_abfs_strg_cont
    account_name = config.hs4v1_abfs_strg_acc
    location = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/user/hive/hocean/hs4v2/{config.hull}/"

    return location


def clear_table():
    # 테이블 및 오브젝트 스토리지 데이터 삭제
    connection = ImpalaConnection()
    conn = connection.conn
    cursor = conn.cursor()

    location = get_storage_location()

    # 테이블 삭제
    sql = f"DROP TABLE IF EXISTS {DB_NAME}.{TABLE_NAME};"
    cursor.execute(sql)

    # 클라우드에서 데이터 삭제
    if abfs.exists(location):
        abfs.rm(location, recursive=True)


# def insert_data(
#     col_names: list[str], metadata_dict: dict[str, MetaData], start: str, end: str
# ):
#     conn = ImpalaConnection().conn

#     # source table에서 데이터 쿼리
#     df = query_data(metadata_dict=metadata_dict, start=start, end=end)
#     df = df.with_columns(pl.from_epoch("ds_timestamp", time_unit="ms"))
#     data = df.to_dicts()

#     # print(df)
#     col_str = ",\n\t".join(col_names)
#     val_str = ",\n\t".join([f"%({col})s" for col in col_names])
#     sql = f"INSERT INTO {DB_NAME}.{TABLE_NAME} ({col_str}) VALUES ({val_str})"

#     logger.debug(sql)

#     cursor = conn.cursor()
#     cursor.executemany(sql, data)

#     return


def insert_data2(
    col_names: list[str], metadata_dict: dict[str, MetaData], start: str, end: str
):
    conn = ImpalaConnection().conn
    cursor = conn.cursor()
    cursor.execute("SET PARQUET_FALLBACK_SCHEMA_RESOLUTION=name")

    base = get_storage_location().replace(f"/{config.hull}/", "")
    tmp_dir = f"{str(base)}/tmpdata"
    tmp_parquet_path = f"{tmp_dir}/{start}_{end}.parquet"
    tmp_tbl_name = f"{config.hull}_{start}_{end}"

    # source table에서 데이터 쿼리
    df = query_data(metadata_dict=metadata_dict, start=start, end=end)
    df = df.with_columns(pl.from_epoch("ds_timestamp", time_unit="ms"))

    # 임시 테이블 생성
    ## parquet 저장 경로 생성
    if not abfs.exists(tmp_dir):
        abfs.makedirs(tmp_dir, exist_ok=True)
        logger.debug(f"Temporary Dataframe has been copied to '{tmp_dir}'.")

    ## Azure에 데이터 프레임 저장
    df.write_parquet(
        tmp_parquet_path,
        storage_options={
            "account_name": config.hs4v1_abfs_strg_acc,
            "account_key": config.hs4v1_abfs_strg_key,
        },
    )

    ## 임시 테이블 생성 및 파티션 연결
    ### 동일한 테이블 이름이 있으면 삭제
    cursor.execute(f"DROP TABLE IF EXISTS tmp.{tmp_tbl_name}")
    ### sql 문작성
    sql = f"""
        CREATE EXTERNAL TABLE tmp.{tmp_tbl_name} (
            {',\n\t'.join(col_names)}
        )
        STORED AS PARQUET
        LOCATION '{tmp_dir}'
    """
    cursor.execute(sql)

    # 임시 테이블에서 데이터 복사
    col_names_str = ",\n\t".join(
        [str_value.split()[0].strip() for str_value in col_names]
    )
    sql = f"""INSERT INTO {DB_NAME}.{TABLE_NAME} ({col_names_str})
    SELECT {col_names_str} FROM tmp.{tmp_tbl_name}
    """
    logger.debug(sql)
    cursor.execute(sql)

    # 임시 테이블 및 데이터 파일 삭제
    # ## 임시 데이터 파일 삭제
    if abfs.exists(tmp_dir):
        abfs.rm(tmp_dir, recursive=True)

    ## 임시 테이블 삭제
    cursor.execute(f"DROP TABLE IF EXISTS tmp.{tmp_tbl_name}")


def create_iceberg_table(metadata_dict: dict[str, MetaData]) -> list[str]:
    # Iceberg 테이블 생성
    # 생성된 테이블의 열을 배열로 반환
    table_name = f"{DB_NAME}.{TABLE_NAME}"
    connection = ImpalaConnection()

    # 클라우드 테이블 저장위치
    location = get_storage_location()

    # column 정의
    cols = ["ds_timestamp    TIMESTAMP"]
    cols.extend(
        [
            f"{m.col_name}    {m.data_type}"
            for metadata_list in metadata_dict.values()
            for m in metadata_list
        ]
    )
    cols.extend([f"id_{table_name}    BIGINT" for table_name in metadata_dict.keys()])
    cols_str = ",\n".join(cols)

    # iceberg 테이블 생성 sql
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {cols_str}
        )
        PARTITIONED BY SPEC (MONTHS(ds_timestamp))
        STORED AS ICEBERG
        LOCATION '{location}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.sort.order' = 'ds_timestamp',
            'write.target-file-size-bytes' = '268435456'
        )
    """

    cursor = connection.conn.cursor()
    cursor.execute(sql)

    logger.info(f"The '{table_name}' table has been successfully created.")

    rs = cursor.fetchall()
    for row in rs:
        logger.debug(row[0])

    return cols


# def copy_data(metadata_list: list[MetaData], start: str, end: str):
#     # 특정 테이블에 있는 데이터를 새로 생성한 Iceberg 테이블로 복사
#     connection = ImpalaConnection()

#     db_name = list(set([m.db_name for m in metadata_list]))
#     table_name = list(set([m.table_name for m in metadata_list]))

#     if not db_name:
#         err_msg = "Not available metadata - db_name is empty."
#         logger.error(err_msg)
#         raise ValueError(err_msg)

#     if len(db_name) > 1:
#         err_msg = "Not available metadata - db_name is not unique."
#         logger.error(err_msg)
#         raise ValueError(err_msg)

#     if not table_name:
#         err_msg = "Not available metadata - table_name is empty."
#         logger.error(err_msg)
#         raise ValueError(err_msg)

#     if len(table_name) != 1:
#         err_msg = "Not available metadata - table_name is not unique."
#         logger.error(err_msg)
#         raise ValueError(err_msg)

#     db_name = db_name[0]
#     table_name = table_name[0]

#     # 복사할 열 세팅
#     cols = list()
#     tags = list()
#     for m in metadata_list:
#         cols.append(m.col_name)

#         # timestamp 형 변환
#         if "timestamp" in m.col_name.lower():
#             tags.append(
#                 f"FROM_UNIXTIME(CAST(CAST({m.tag}    AS    BIGINT) / 1000 AS BIGINT)) AS ds_timestamp"
#             )
#             continue

#         # id 형 변환
#         if "id" in m.col_name.lower():
#             tags.append(f"CAST(CAST(ds_timestamp    AS    DOUBLE) AS BIGINT) AS id")
#             continue

#         # 메타데이터 정의대로 형 변환
#         tags.append(f"CAST({m.tag}    AS    {m.data_type})")

#     cols = ",\n".join(cols)
#     tags = ",\n".join(tags)

#     # ias_no1 데이터 복사
#     sql = f"""
#         INSERT INTO {DB_NAME}.{TABLE_NAME} (
#             {cols}
#         )
#         SELECT
#             {tags}
#         FROM {db_name}.{table_name}
#         WHERE ds_date BETWEEN '{start}' AND '{end}'
#         ORDER BY ds_timestamp
#        -- limit 10;
#     """

#     # print(sql)
#     # logger.debug(sql)

#     cursor = connection.conn.cursor()
#     cursor.execute(sql)

#     logger.info(f"Data from the '{table_name}' table has been successfully copied.")


def query_data(
    metadata_dict: dict[str, list[MetaData]], start: str, end: str
) -> pl.DataFrame:
    # 기존 테이블에서 데이터 쿼리

    conn = ImpalaConnection()
    df_set = dict()

    for table_name, metadata_list in metadata_dict.items():
        col_map = {"ds_timestamp": f"id_{table_name}"}

        tags = [f"CAST(CAST(ds_timestamp    AS    DOUBLE) AS BIGINT) AS ds_timestamp"]

        if not metadata_list:
            logger.warning(f"Empty metadata list - {table_name}. Skip to next table.")
            continue

        db_name = get_db_name(metadata_dict)

        # 쿼리문 생성
        for m in metadata_list:
            if "ds_timestamp" == m.col_name.lower():
                continue

            # id 형 변환
            if "id" == m.col_name.lower():
                tags.append(f"CAST(CAST(ds_timestamp    AS    DOUBLE) AS BIGINT) AS id")
                continue

            # 메타데이터 정의대로 형 변환
            tags.append(f"CAST({m.tag}    AS    {m.data_type})    AS    {m.tag}")
            col_map[m.tag] = m.col_name

        sql = f"""
            SELECT
            {',\n\t'.join(tags)}
            FROM
            {db_name}.{table_name}
            WHERE ds_date BETWEEN '{start}' AND '{end}';
        """

        # logger.debug(sql)

        # 데이터 쿼리
        df = conn.query_polars(sql=sql)
        # 32비트 변수로 변경
        df = df.with_columns(
            [
                pl.col(pl.Int64).exclude("ds_timestamp").cast(pl.Int32),
                pl.col(pl.Float32).cast(pl.Float64),
            ]
        )

        if df.is_empty():
            logger.warning(f"The query result set for table '{table_name}' is empty.")

        # tag 이름 column 이름으로 맵핑
        df = df.rename(col_map)

        # 15초 간격 리샘플
        synced_df = (
            df.with_columns(
                pl.from_epoch(pl.col(f"id_{table_name}"), time_unit="ms")
                .dt.round("15s")
                .alias("time_sync"),
            )
            .group_by("time_sync")
            .agg(pl.all().first())
            .sort("time_sync")
        )

        df_set[table_name] = synced_df

        combined_df = reduce(
            lambda left, right: left.join(right, on="time_sync", how="left"),
            list(df_set.values()),
        ).with_columns(
            pl.col("time_sync").cast(pl.Int64).alias("ds_timestamp"),
        )

        # df = df.with_columns([
        #     pl.col(pl.Int64).cast(pl.Int32),
        #     pl.col(pl.Int32).cast(pl.Int32), # 이미 32인 것도 확인
        #     pl.col(pl.Float32).cast(pl.Float64)
        # ])

    return combined_df


def main():
    # 메타데이터 읽기
    metadata_list = read_metadata(f"resources/csv/{config.hull}_metadata.csv")

    # 기존 테이블 및 데이터 삭제
    clear_table()

    # Iceberg 테이블 생성
    col_names = create_iceberg_table(metadata_list)

    insert_data2(col_names, metadata_list, start="20260101", end="20260301")
    insert_data2(col_names, metadata_list, start="20260302", end="20260320")


if __name__ == "__main__":
    main()
