import inspect
import polars as pl
import time
from config import config, logger
from connection import ImpalaConnection, abfs
from time import perf_counter
from datetime import datetime, timedelta
from utils import metadata, data_loader
from utils.consts import DB_NAME, TABLE_NAME, MS_TO_KN


def create_iceberg_table(metadata_dict: dict[str, metadata.MetaData]) -> list[str]:
    """
    Iceberg 테이블 생성
    - 생성된 테이블의 열을 배열로 반환
    """
    start = perf_counter()
    logger.info(f"👉 Create an iceberg table named '{DB_NAME}.{TABLE_NAME}'.")
    suffix = "raw"
    table_name = f"{DB_NAME}.{TABLE_NAME}"
    connection = ImpalaConnection()

    # Azure 오브젝트 스토리지의 테이블 위치
    location = f"{config.storage_location}/{suffix}"

    # column 정의
    cols = ["ds_timestamp    TIMESTAMP"]
    cols.extend(
        sorted(
            [
                f"{m.col_name}    {m.data_type}"
                for metadata_list in metadata_dict.values()
                for m in metadata_list
            ]
        )
    )
    # 예외처리: 호선에 따른 열
    cols = metadata.append_additional_cols(cols)
    cols[1:] = sorted(cols[1:])
    cols.extend([f"id_{table_name}    BIGINT" for table_name in metadata_dict.keys()])
    cols_str = ",\n".join(cols)

    # iceberg 테이블 생성 sql
    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {cols_str}
        )
        PARTITIONED BY SPEC (YEARS(ds_timestamp))
        STORED AS ICEBERG
        LOCATION '{location}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.sort.order' = 'ds_timestamp',
            'write.target-file-size-bytes' = '268435456'
        )
    """

    try:
        cursor = connection.conn.cursor()
        cursor.execute(sql)

        rs = cursor.fetchall()
        for row in rs:
            logger.debug(f"  - {row[0]}")

        logger.info(f"   - elapsed time: {perf_counter() - start: .2f} (sec)")
    except Exception as e:
        logger.error(f"❌ {e} [{__file__}:{inspect.currentframe().f_lineno}]")
        raise e

    return cols


def create_calc_table():
    """
    RAW 데이터로부터 계산이 필요한 데이터를 저장할 테이블 생성
    """
    # 계산 테이블 생성
    suffix = "calc"
    hnum = config.hull
    tbl_name = f"{hnum}_{suffix}"
    location = f"{config.storage_location}/{suffix}"
    connection = ImpalaConnection()
    logger.info(f"👉 Create the calcuation table: '{DB_NAME}.{tbl_name}'")

    # cols = [
    #     {"ds_timestap": "TIMESTAMP"},
    #     {"beaufort_number": "INTEGER"},
    #     {"foc", "FLOAT"},
    #     {"fgc", "FLOAT"},
    # ]

    sql = f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}.{tbl_name} (
            ds_timestamp    TIMESTAMP,
            true_wind_speed    FLOAT,
            true_wind_angle    FLOAT,
            beaufort_number    INTEGER,
            foc    FLOAT,
            fgc    FLOAT
        )
        PARTITIONED BY SPEC (YEARS(ds_timestamp))
        STORED AS ICEBERG
        LOCATION '{location}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.sort.order' = 'ds_timestamp',
            'write.target-file-size-bytes' = '268435456'
        )
    """

    try:
        cursor = connection.conn.cursor()
        cursor.execute(sql)

        rs = cursor.fetchall()
        for row in rs:
            logger.debug(f"  - {row[0]}")
    except Exception as e:
        logger.error(f"❌ {e} [{__file__}:{inspect.currentframe().f_lineno}]")
        raise e

    return


def create_temporary_table(params: dict[str, str | pl.DataFrame]):
    """
    데이터프레임을 임시 테이블로 생성
    - 향후 TARGET 테이블과 병합
    """
    start = perf_counter()
    table_name = params["tbl_name"]
    col_names = params["col_names"]
    location = params["location"]
    parquet_path = params["parquet_path"]
    df = params["df"]

    try:
        conn = ImpalaConnection().conn
        cursor = conn.cursor()

        # 임시 테이블 생성
        ## parquet 저장 경로 생성
        if not abfs.exists(location):
            abfs.makedirs(location, exist_ok=True)
            logger.debug(f"  - Temporary Dataframe has been copied to '{location}'.")

        ## Azure에 parquet 저장
        df.write_parquet(
            parquet_path,
            storage_options=config.get_storage_options(),
        )

        cursor.execute(f"DROP TABLE IF EXISTS tmp.{table_name}")

        ### 테이블 생성 sql 문작성
        sql = f"""
            CREATE EXTERNAL TABLE tmp.{table_name} (
                {',\n\t'.join(col_names)}
            )
            STORED AS PARQUET
            LOCATION '{location}'
        """
        cursor.execute(sql)

        logger.info(f"   - Create the temporary table 'tmp.{table_name}'")
        logger.info(f"     * elapsed time: {perf_counter() - start:.2f} (sec)")
    except Exception as e:
        logger.error(f"❌ {e} [{__file__}:{inspect.currentframe().f_lineno}]")
        raise e

    return


def merge_tables(params: dict[str, str]):
    """
    임시 테이블에 저장된 데이터를 TARGET 테이블로 복사
    - params에 TARGET 테이블 이름 필요
    """
    start = perf_counter()
    col_names = params["col_names"]
    table_name = params["tbl_name"]

    cols = [str_value.split()[0].strip() for str_value in col_names]
    col_names_str = ",\n\t".join(cols)

    try:
        conn = ImpalaConnection().conn
        cursor = conn.cursor()
        cursor.execute("SET PARQUET_FALLBACK_SCHEMA_RESOLUTION=name")

        # 임시 테이블에서 데이터 복사
        sql = f"""
            MERGE INTO {DB_NAME}.{TABLE_NAME} TARGET
            USING tmp.{table_name} AS SOURCE
            ON TARGET.ds_timestamp = SOURCE.ds_timestamp
            WHEN MATCHED THEN
                UPDATE SET
                    {',\n\t'.join([f"TARGET.{col} = SOURCE.{col}" for col in cols])}
            WHEN NOT MATCHED THEN
                INSERT ({col_names_str})
                VALUES ({',\n\t'.join([f"SOURCE.{col}" for col in cols])})
        """

        # sql = f"""
        #     INSERT INTO {DB_NAME}.{TABLE_NAME} ({col_names_str})
        #     SELECT {col_names_str} FROM tmp.{tmp_tbl_name}
        #     ORDER BY ds_timestamp
        # """
        # logger.debug(sql)
        cursor.execute(sql)

        logger.info(f"   - Merge the temporary table 'tmp.{table_name}'")
        logger.info(f"     * elapsed time: {perf_counter() - start:.2f} (sec)")
    except Exception as e:
        logger.error(f"❌ {e} [{__file__}:{inspect.currentframe().f_lineno}]")
        raise e

    return


def drop_temporary_table(params: dict[str, str]):
    """
    임시 테이블 삭제
    - DROP TABLE로 메타데이터 삭제
    - 데이터 파일 삭제
    """
    start = perf_counter()
    table_name = params["tbl_name"]
    location = params["location"]

    try:
        conn = ImpalaConnection().conn
        cursor = conn.cursor()

        # 임시 테이블 및 데이터 파일 삭제
        # ## 임시 데이터 파일 삭제
        if abfs.exists(location):
            abfs.rm(location, recursive=True)
            logger.debug(f"  - Delete the temporary file '{location}'.")

        ## 임시 테이블 삭제
        cursor.execute(f"DROP TABLE IF EXISTS tmp.{table_name}")
        logger.debug(f"  - Drop the temporary table")

        logger.info(f"   - Drop the temporary table 'tmp.{table_name}'")
        logger.info(f"     * elapsed time: {perf_counter() - start:.2f} (sec)")
    except Exception as e:
        logger.error(f"❌ {e} [{__file__}:{inspect.currentframe().f_lineno}]")
        raise e

    return


def insert_calc_data(start: str, end: str):
    suffix = "calc"
    hnum = config.hull
    tgt_tbl = f"{hnum}_{suffix}"
    src_tbl = TABLE_NAME
    dt_format = "%Y-%m-%d 00:00:00"
    connection = ImpalaConnection()

    start = datetime.strptime(start, "%Y%m%d").strftime(dt_format)
    end = (datetime.strptime(end, "%Y%m%d") + timedelta(days=1)).strftime(dt_format)

    sql = f"""
        MERGE INTO {DB_NAME}.{tgt_tbl} AS target
        USING (
            WITH vector_base AS (
                SELECT
                    ds_timestamp,
                    vdr_relative_wind_speed, -- unit: m/s
                    vdr_relative_wind_angle, -- unit: degree
                    vdr_aivdo_sog,
                    -{MS_TO_KN} * vdr_relative_wind_speed * SIN(RADIANS(vdr_relative_wind_angle)) AS v_tx,
                    -{MS_TO_KN} * vdr_relative_wind_speed * COS(RADIANS(vdr_relative_wind_angle)) + vdr_aivdo_sog AS v_ty,
                    me_fo_flow + ge_fo_flow + auxb_fo_flow AS foc_raw,
                    me_fg_flow + ge_fg_flow AS fgc_raw
                FROM {DB_NAME}.{src_tbl}
                WHERE ds_timestamp >= CAST('{start}' AS TIMESTAMP)
                AND ds_timestamp < CAST('{end}' AS TIMESTAMP)
            ),
            true_wind_calc AS (
                SELECT
                    ds_timestamp,
                    SQRT(POWER(v_tx, 2) + POWER(v_ty, 2)) AS v_t,
                    (DEGREES(ATAN2(v_tx, v_ty)) + 360) % 360 AS v_d,
                    foc_raw,
                    fgc_raw
                FROM vector_base
            )
            SELECT
                ds_timestamp,
                CAST(v_t AS FLOAT) AS true_wind_speed,
                CAST(v_d AS FLOAT) AS true_wind_angle,
                CASE
                    WHEN v_t IS NULL THEN NULL
                    WHEN v_t < 1.0  THEN 0
                    WHEN v_t < 4.0  THEN 1
                    WHEN v_t < 7.0  THEN 2
                    WHEN v_t < 11.0 THEN 3
                    WHEN v_t < 17.0 THEN 4
                    WHEN v_t < 22.0 THEN 5
                    WHEN v_t < 28.0 THEN 6
                    WHEN v_t < 34.0 THEN 7
                    WHEN v_t < 41.0 THEN 8
                    WHEN v_t < 48.0 THEN 9
                    WHEN v_t < 56.0 THEN 10
                    WHEN v_t < 64.0 THEN 11
                    ELSE 12
                END AS beaufort_number,
                CAST(foc_raw AS FLOAT) AS foc,
                CAST(fgc_raw AS FLOAT) AS fgc
            FROM true_wind_calc
        ) AS source
        ON target.ds_timestamp = source.ds_timestamp -- 중복 판단 기준
        WHEN MATCHED THEN
            UPDATE SET
                target.true_wind_speed = source.true_wind_speed,
                target.true_wind_angle = source.true_wind_angle,
                target.beaufort_number = source.beaufort_number,
                target.foc = source.foc,
                target.fgc = source.fgc
        WHEN NOT MATCHED THEN
            INSERT (ds_timestamp, true_wind_speed, true_wind_angle, beaufort_number, foc, fgc)
            VALUES (source.ds_timestamp, source.true_wind_speed, source.true_wind_angle, source.beaufort_number, source.foc, source.fgc);
    """

    try:
        cursor = connection.conn.cursor()
        cursor.execute(sql)
    except Exception as e:
        logger.error(f"❌ {e} [{__file__}:{inspect.currentframe().f_lineno}]")
        raise e

    return


def insert_data(
    col_names: list[str],
    metadata_dict: dict[str, metadata.MetaData],
    start: str,
    end: str,
    add_cols: bool = True
):
    logger.info(f"👉 Insert the data into '{DB_NAME}.{TABLE_NAME}'")

    # 임시 parquet 저장위치 설정
    base = config.storage_location.replace(f"/{config.hull}/", "")
    tmp_dir = f"{str(base)}/tmpdata"
    tmp_parquet_path = f"{tmp_dir}/{start}_{end}.parquet"
    tmp_tbl_name = f"{config.hull}_{start}_{end}"

    # source table에서 데이터 쿼리
    # df = query_data(metadata_dict=metadata_dict, start=start, end=end)
    df = data_loader.query_data(metadata_dict=metadata_dict, start=start, end=end)

    # todo: 호선 별 예외처리
    if add_cols:
        df = metadata.caculate_additional_cols(df)

    # # 임시 테이블 생성
    create_temporary_table(
        {
            "tbl_name": tmp_tbl_name,
            "col_names": col_names,
            "location": tmp_dir,
            "parquet_path": tmp_parquet_path,
            "df": df,
        }
    )

    time.sleep(1)

    # # 임시 생성된 테이블로부터 데이터 복사
    merge_tables({"col_names": col_names, "tbl_name": tmp_tbl_name})

    time.sleep(1)

    # # 임시 테이블 및 데이터 파일 삭제
    drop_temporary_table({"tbl_name": tmp_tbl_name, "location": tmp_dir})

    # # 계산 테이블 생성: 원본 데이터가 저장된 테이블로부터 계산이 요구되는 테이블 생성
    create_calc_table()

    time.sleep(1)

    insert_calc_data(start, end)

    return


def clear_table():
    # 테이블 및 오브젝트 스토리지 데이터 삭제
    start = perf_counter()
    hnum = config.hull
    location = config.storage_location
    logger.info(f"👉 Clear exsiting table for '{hnum}'.")

    try:
        connection = ImpalaConnection()
        conn = connection.conn
        cursor = conn.cursor()

        # 호선 이름이 포함된 테이블 찾기
        sql = f"SHOW TABLES IN {DB_NAME} LIKE '{hnum.lower()}*'"
        cursor.execute(sql)
        for (table,) in cursor.fetchall():
            # 테이블 삭제
            sql = f"DROP TABLE IF EXISTS {DB_NAME}.{table};"
            logger.debug(f"  - Drop table: {table}")
            cursor.execute(sql)

        # 클라우드에서 데이터 삭제
        if abfs.exists(location):
            abfs.rm(location, recursive=True)

        logger.info(f"   - elapsed time: {perf_counter() - start: .2f} (sec)")
    except Exception as e:
        logger.error(f"❌ {e} [{__file__}:{inspect.currentframe().f_lineno}]")
        raise e
