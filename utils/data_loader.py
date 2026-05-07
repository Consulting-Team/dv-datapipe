import inspect
import polars as pl
from connection import ImpalaConnection
from functools import reduce
from utils.metadata import *
from time import perf_counter


def query_data(
    metadata_dict: dict[str, list[MetaData]], start: str, end: str
) -> pl.DataFrame:
    """기존 Hive 테이블에서 데이터 쿼리"""
    start_time = perf_counter()
    logger.info(f"   - Query data from '{start}' to '{end}'")

    df_list = list()
    conn = ImpalaConnection()

    try:
        for table_name, metadata_list in metadata_dict.items():
            col_map = {"ds_timestamp": f"id_{table_name}"}

            tags = [
                f"CAST(CAST(ds_timestamp    AS    DOUBLE) AS BIGINT) AS ds_timestamp"
            ]

            if not metadata_list:
                logger.warning(
                    f"- ⚠️ Empty metadata list - {table_name}. Skip to next table."
                )
                continue

            db_name = get_db_name(metadata_dict)

            # 쿼리문 생성
            for m in metadata_list:
                ## 메타데이터 정의대로 형 변환
                match m.data_type:
                    ## BOOLEAN 타입일 경우 예외 처리
                    case "BOOLEAN":
                        tags.append(f"""CASE
                                    WHEN LOWER({m.tag}) IN ('true', '1') THEN TRUE
                                    WHEN LOWER({m.tag}) IN ('false', '0') THEN FALSE
                                ELSE NULL
                                END AS {m.tag}
                            """)
                    case _:
                        tags.append(
                            f"CAST({m.tag}    AS    {m.data_type})    AS    {m.tag}"
                        )

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
            if "vdr" in table_name:
                df = conn.query_polars(sql=sql, idx_access=False)
            else:
                df = conn.query_polars(sql=sql)
            logger.info(f"     * table: {table_name}")

            if df.is_empty():
                logger.warning(
                    f"  * ⚠️ The query result set for table '{table_name}' is empty."
                )

            # 15초 간격 리샘플
            synced_df = (
                df.rename(col_map)
                .with_columns(
                    pl.from_epoch(pl.col(f"id_{table_name}"), time_unit="ms")
                    # .dt.round("15s")
                    .dt.truncate("15s").alias("ds_timestamp"),
                )
                .sort("ds_timestamp")
                .group_by("ds_timestamp")
                .agg(pl.all().first())
                .with_columns(pl.col("ds_timestamp").cast(pl.Datetime("ms")))
            )

            df_list.append(synced_df)

        # 데이터프레임 조인
        combined_df: pl.DataFrame = reduce(
            lambda left, right: left.join(right, on="ds_timestamp", how="left"),
            df_list,
        )

        # 스키마 입히기
        schema = get_schema(metadata_dict)
        combined_df = combined_df.cast(schema)

        logger.info(f"     * elapsed time: {perf_counter() - start_time: .2f} (sec)")
    except Exception as e:
        logger.error(f"{e} [{__file__}:{inspect.currentframe().f_lineno}]")
        raise e

    return combined_df


def get_columns(db_name: str, table_name: str) -> pl.DataFrame:
    """
    테이블에서 column 읽어와서 반환
    """

    conn = ImpalaConnection()
    sql = f"DESCRIBE {db_name}.{table_name}"

    try:
        df = conn.query_polars(sql=sql)
        return df
    except Exception as e:
        logger.error(f"{e} [{__file__}:{inspect.currentframe().f_lineno}]")
        raise e