#! H2521호선 iceberg 테이블 생성 테스트

from config import config
from jmlogger import logger
from connection import ImpalaConnection, abfs
from utils.metadata import read_metadata, MetaData, get_db_name
from functools import reduce
import polars as pl
from datetime import timedelta

# 테이블이 생성될 DB
DB_NAME = "tmp"
# 새로 생성할 테이블 이름
TABLE_NAME = f"{config.hull}_iceberg_test"


def get_storage_location() -> str:
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


def create_iceberg_table(metadata_list: dict[str, MetaData]):
    # Iceberg 테이블 생성
    table_name = f"{DB_NAME}.{TABLE_NAME}"
    connection = ImpalaConnection()

    # 클라우드 테이블 저장위치
    location = get_storage_location()

    # column 정의
    cols = [
        f"{m.col_name}    {m.data_type}"
        for data in metadata_list.values()
        for m in data
    ]
    cols = ",\n".join(cols)

    # iceberg 테이블 생성 sql
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {cols}
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


def copy_data_join(metadata_list: dict[str, MetaData]):
    # id(timestamp)로 joint하여 Iceberg 테이블에 데이터 복사
    connection = ImpalaConnection()

    target_table = f"{DB_NAME}.{TABLE_NAME}"
    join_clauses = []
    select_columns = []

    for i, (table_name, metadata) in enumerate(metadata_list.items()):
        alias = f"s{i}"
        tags = list()
        # 각 소스 테이블에서 가져올 컬럼들에 CAST 적용
        for m in metadata:
            # if "src.ds_timestamp" not in tags:
            tags.append(f"src.{m.tag}")
            select_columns.append(f"CAST({alias}.{m.tag} AS {m.data_type})")

        tags_str = ",\n".join(list(set(tags)))

        # ±10초(10000ms) 내에서 가장 가까운 행 1개만 매칭 (그 외에는 자동으로 NULL)
        join_sql = f"""
            LEFT JOIN (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY ref_id ORDER BY diff ASC) AS rn
                FROM (
                    SELECT
                        {tags_str},
                        t_ref.id AS ref_id,
                        ABS(t_ref.id - CAST(src.ds_timestamp AS DOUBLE)) AS diff
                    FROM {target_table} t_ref
                    JOIN {"hlng"}.{table_name} src
                    ON CAST(src.ds_timestamp AS DOUBLE) BETWEEN (t_ref.id - 10000) AND (t_ref.id + 10000)
                ) AS tmp_{i}
            ) AS {alias} ON t.id = {alias}.ref_id AND {alias}.rn = 1
        """
        join_clauses.append(join_sql)

    sql = f"""
        INSERT OVERWRITE {target_table}
        SELECT DISTINCT {', '.join(select_columns)}
        FROM {target_table} t
        {' '.join(join_clauses)}
    """
    logger.debug(sql)

    cursor = connection.conn.cursor()
    cursor.execute(sql)


def copy_data(metadata_list: list[MetaData], start: str, end: str):
    # 특정 테이블에 있는 데이터를 새로 생성한 Iceberg 테이블로 복사
    connection = ImpalaConnection()

    db_name = list(set([m.db_name for m in metadata_list]))
    table_name = list(set([m.table_name for m in metadata_list]))

    if not db_name:
        err_msg = "Not available metadata - db_name is empty."
        logger.error(err_msg)
        raise ValueError(err_msg)

    if len(db_name) > 1:
        err_msg = "Not available metadata - db_name is not unique."
        logger.error(err_msg)
        raise ValueError(err_msg)

    if not table_name:
        err_msg = "Not available metadata - table_name is empty."
        logger.error(err_msg)
        raise ValueError(err_msg)

    if len(table_name) != 1:
        err_msg = "Not available metadata - table_name is not unique."
        logger.error(err_msg)
        raise ValueError(err_msg)

    db_name = db_name[0]
    table_name = table_name[0]

    # 복사할 열 세팅
    cols = list()
    tags = list()
    for m in metadata_list:
        cols.append(m.col_name)

        # timestamp 형 변환
        if "timestamp" in m.col_name.lower():
            tags.append(
                f"FROM_UNIXTIME(CAST(CAST({m.tag}    AS    BIGINT) / 1000 AS BIGINT)) AS ds_timestamp"
            )
            continue

        # id 형 변환
        if "id" in m.col_name.lower():
            tags.append(f"CAST(CAST(ds_timestamp    AS    DOUBLE) AS BIGINT) AS id")
            continue

        # 메타데이터 정의대로 형 변환
        tags.append(f"CAST({m.tag}    AS    {m.data_type})")

    cols = ",\n".join(cols)
    tags = ",\n".join(tags)

    # ias_no1 데이터 복사
    sql = f"""
        INSERT INTO {DB_NAME}.{TABLE_NAME} (
            {cols}
        )
        SELECT
            {tags}
        FROM {db_name}.{table_name}
        WHERE ds_date BETWEEN '{start}' AND '{end}'
        ORDER BY ds_timestamp
       -- limit 10;
    """

    # print(sql)
    # logger.debug(sql)

    cursor = connection.conn.cursor()
    cursor.execute(sql)

    logger.info(f"Data from the '{table_name}' table has been successfully copied.")


# def copy_data_test(metadata_list: list[MetaData], start: str, end: str) -> pl.DataFrame:
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

#     if len(table_name) > 1:
#         err_msg = "Not available metadata - table_name is not unique."
#         logger.error(err_msg)
#         raise ValueError(err_msg)

#     db_name = db_name[0]
#     table_name = table_name[0]

#     # ---------------------------------------------------------------------------------------------------
#     col_map = dict()
#     tags = list()

#     tags.append(f"CAST(CAST(ds_timestamp    AS    DOUBLE) AS BIGINT) AS ds_timestamp")

#     for m in metadata_list:
#         # timestamp 형 변환
#         if "ds_timestamp" == m.col_name.lower():
#             # tags.append(
#             #     f"CAST(CAST(ds_timestamp    AS    DOUBLE) AS BIGINT) AS ds_timestamp"
#             # )
#             continue

#         # id 형 변환
#         if "id" == m.col_name.lower():
#             tags.append(f"CAST(CAST(ds_timestamp    AS    DOUBLE) AS BIGINT) AS id")
#             continue

#         # 메타데이터 정의대로 형 변환
#         tags.append(f"CAST({m.tag}    AS    {m.data_type})    AS    {m.tag}")
#         col_map[m.tag] = m.col_name

#     tags = ",\n\t".join(tags)

#     # ? 데이터프레임으로 로드 후 리샘플링해서 적재
#     sql = f"""
#         SELECT
#         {tags}
#         FROM
#         {db_name}.{table_name}
#         WHERE ds_date BETWEEN '{start}' AND '{end}';
#     """
#     # print(sql)
#     df = connection.query_polars(sql=sql)
#     df = df.sort("ds_timestamp")
#     # tag 매핑
#     df = df.rename(col_map)

#     synced_df = (
#         df.with_columns(
#             pl.from_epoch(pl.col("ds_timestamp"), time_unit="ms")
#             .dt.round("15s")
#             .alias("time_sync")
#         )
#         .group_by("time_sync")
#         .agg(pl.all().first())
#         .sort("time_sync")
#     )

#     # print(synced_df)
#     return synced_df
#     # ---------------------------------------------------------------------------------------------------


def query_data(
    metadata_dict: dict[str, list[MetaData]], departure: str, arrival: str
) -> pl.DataFrame:
    # 기존 테이블에서 데이터 쿼리

    conn = ImpalaConnection()
    df_set = dict()

    for table_name, metadata_list in metadata_dict.items():
        col_map = dict()

        tags = [
            f"CAST(CAST(ds_timestamp    AS    DOUBLE) AS BIGINT) AS ds_timestamp",
            f"CAST(CAST(ds_timestamp    AS    DOUBLE) AS BIGINT) AS id_{table_name}",
        ]

        if not metadata_list:
            logger.warning(f"Empty metadata list - {table_name}. Skip to next table.")
            continue

        db_name = get_db_name(metadata_dict)

        # 쿼리문 생성
        for m in metadata_list:
            # 메타데이터 정의대로 형 변환
            tags.append(f"CAST({m.tag}    AS    {m.data_type})    AS    {m.tag}")
            col_map[m.tag] = m.col_name

        sql = f"""
            SELECT
            {',\n\t'.join(tags)}
            FROM
            {db_name}.{table_name}
            WHERE ds_date BETWEEN '{departure}' AND '{arrival}';
        """

        # 데이터 쿼리
        df = conn.query_polars(sql=sql)

        # 쿼리 결과가 비어있을 경우
        if df.is_empty():
            logger.warning(f"The query result set for table '{table_name}' is empty.")

        # tag 이름 column 이름으로 맵핑
        df = df.rename(col_map)
        df = df.with_columns(
            # pl.from_epoch(pl.col(f"id_{table_name}"), time_unit="ms")
            pl.col("ds_timestamp").cast(pl.Int64),
        # nearest joint을 위해 반드시 소팅 필요
        ).sort("ds_timestamp")

        # 15초 간격 리샘플
        # synced_df = (
        #     df.with_columns(
        #         pl.from_epoch(pl.col(f"id_{table_name}"), time_unit="ms")
        #         .dt.round("15s")
        #         .alias("time_sync")
        #     )
        #     .group_by("time_sync")
        #     .agg(pl.all().first())
        #     .sort("time_sync")
        # )
        # df_set[table_name] = synced_df

        df_set[table_name] = df

    # 테이블 별 데이터프레임 열 방향으로 조인
    # combined_df = reduce(
    #     lambda left, right: left.join(right, on="time_sync", how="left"),
    #     list(df_set.values()),
    # )
    combined_df = reduce(
        lambda left, right: left.join_asof(
            right,
            on="ds_timestamp",
            strategy="nearest",
            tolerance=15000,
        ),
        df_set.values(),
    )

    return combined_df


def main():
    # 메타데이터 읽기
    metadata_list = read_metadata(f"resources/csv/{config.hull}_metadata.csv")

    # # 기존 테이블 및 데이터 삭제
    # clear_table()

    # Iceberg 테이블 생성
    # create_iceberg_table(metadata_list)

    # ias_no1_pivot 테이블만 데이터 적재
    # copy_data(
    #     metadata_list["h2521_ias_ias_no1_pivot"], start="20260201", end="20260301"
    # )

    df = query_data(metadata_list, departure="20260201", arrival="20260201")
    logger.debug(df)
    df.write_csv("combined.csv")

    # 테스트용
    # df = copy_data_test(
    #     metadata_list["h2521_ias_ias_no1_pivot"], start="20260201", end="20260202"
    # )
    # print(df)

    # df = copy_data_test(
    #     metadata_list["h2521_ias_ias_no2_pivot"], start="20260201", end="20260202"
    # )
    # print(df)

    # df = copy_data_test(
    #     metadata_list["h2521_vdr_vdvbw_pivot"], start="20260201", end="20260202"
    # )
    # print(df)

    # df = copy_data_test(
    #     metadata_list["h2521_vdr_sddpt_pivot"], start="20260201", end="20260202"
    # )
    # print(df)

    # ias_no2_pivot 테이블 조인해서 데이터 적재
    # copy_data_join(metadata_list)


if __name__ == "__main__":
    main()
