#! Impala column 유연성 테스트코드
#! 1. 데이터프레임 생성
#! 2. Azure blob에 데이터프레임 업로드
#!  - abfss://data/user/hive/hocean/hs4v2/H0000/dt=1/file1.parquet
#!  - abfss://data/user/hive/hocean/hs4v2/H0000/dt=2/file2.parquet
#! 3. Impala 테스트 테이블 생성
#!  - tmp.col_test_case1
#!  - tmp.col_test_case2
#! 4. 파티션 추가

import polars as pl
from config import config
from jmlogger import logger
from connection import ImpalaConnection

BASE = f"{config.hs4v1_abfs_strg_cont}/user/hive/hocean"


def upload_parquet(dataset: list[pl.DataFrame]):
    #! Azure blob에 데이터프레임 parquet로 저장
    storage_options = {
        "account_name": config.hs4v1_abfs_strg_acc,
        "account_key": config.hs4v1_abfs_strg_key,
    }

    for data in dataset:
        try:
            df = data["df"]
            url = data["target_url"]
            # Blob에 저장
            df.write_parquet(url, storage_options=storage_options)
            # df.write_iceberg(url, storage_options=storage_options)
            logger.info(f"datafile file has ben copied to: {url}")
        except Exception as e:
            logger.error(e)
            raise e

    return


def create_table(schema: dict) -> list[str]:
    #! Impala 테이블 생성 및 파티션 추가
    conn = ImpalaConnection()
    db_name = "tmp"
    table_names = ["col_test_case1", "col_test_case2"]

    if conn.exists_db("tmp"):
        container = config.hs4v1_abfs_strg_cont
        account = config.hs4v1_abfs_strg_acc
        partition_names = ["dt=1", "dt=2"]
        # schema = {"cola": pl.Float32, "colb": pl.Float32}

        # 이미 테이블이 있을 경우 삭제
        conn.drop_table(
            {
                "db_name": db_name,
                "table_names": table_names,
            }
        )

        for table_name in table_names:
            # external table 생성
            conn.create_external_table(
                {
                    "db_name": db_name,
                    "table_name": table_name,
                    "schema": schema,
                    "location": f"abfss://{container}@{account}.dfs.core.windows.net/user/hive/hocean/hs4v2/H0000/",
                }
            )

            # partion 추가
            for partition_name in partition_names:
                conn.add_partition(
                    {
                        "db_name": db_name,
                        "table_name": table_name,
                        "partition_name": partition_name,
                        "location": f"abfss://{container}@{account}.dfs.core.windows.net/user/hive/hocean/hs4v2/H0000/{partition_name}/",
                    }
                )

    # for table_name in table_names:
    #     df = conn.query_polars(f"SELECT dt, cola, colb FROM {db_name}.{table_name};")
    #     print(df)

    return table_names

def create_iceberg_table(schema: dict[str, str]) -> list[str]:
    #! Impala iceberg 테이블 생성
    conn = ImpalaConnection()
    db_name = "tmp"
    table_names = ["col_test_iceberg"]

    if conn.exists_db("tmp"):
        container = config.hs4v1_abfs_strg_cont
        account = config.hs4v1_abfs_strg_acc

        # 이미 테이블이 있을 경우 삭제
        conn.drop_table(
            {
                "db_name": db_name,
                "table_names": table_names,
            }
        )

        for table_name in table_names:
            # external table 생성
            conn.create_external_iceberg_table(
                {
                    "db_name": db_name,
                    "table_name": table_name,
                    "schema": schema,
                    "location": f"abfss://{container}@{account}.dfs.core.windows.net/user/hive/hocean/hs4v2/H0000",
                }
            )

    return table_names


def main():
    # 테스트용 parquet 파일 생성
    schema = {"cola": pl.Float32, "colb": pl.Float32}

    df1 = pl.DataFrame(
        {"cola": [1.0, 1.0, 1.0], "colb": [2.0, 2.0, 2.0]},
        schema={"cola": pl.Float32, "colb": pl.Float32},
    )

    df2 = pl.DataFrame(
        {"colb": [2.0, 2.0, 2.0], "cola": [1.0, 1.0, 1.0], "colc": [0.0, 0.0, 0.0]},
        schema={"colc": pl.Float32, "colb": pl.Float32, "cola": pl.Float32},
    )

    # print(df1)
    # print(df2)

    path = f"abfss://{BASE}/hs4v2/H0000/"

    dataset = [
        {"df": df1, "target_url": path + "dt=1/file1.parquet"},
        {"df": df2, "target_url": path + "dt=2/file2.parquet"},
    ]

    # 리펙토링
    upload_parquet(dataset)

    # 테이블 생성
    table_names = create_table(schema)

    # # iceberg 테이블 생성
    # iceberg_table_names = create_iceberg_table(schema)

    # table_names.extend(iceberg_table_names)


    # 테이블 조회
    query_tables(table_names)


def query_tables(table_names: list[str]):
    #! Impala 테이블 조회
    conn = ImpalaConnection().conn
    cursor = conn.cursor()

    cursor.execute("SET PARQUET_FALLBACK_SCHEMA_RESOLUTION = name")

    for table_name in table_names:
        cursor.execute(f"SELECT dt, cola, colb FROM tmp.{table_name};")

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pl.DataFrame(rows, schema=columns, orient="row")

        print(f"{'-' * 10}table: {table_name}{'-' * 10}")
        print(df)


def show_parquet():
    #! 테스트용 parquet 파일 조회
    storage_options = {
        "account_name": config.hs4v1_abfs_strg_acc,
        "account_key": config.hs4v1_abfs_strg_key,
    }

    df1 = pl.read_parquet(
        "abfss://data@dsmecdpseoul.dfs.core.windows.net/user/hive/hocean/hs4v2/H0000/dt=1/file1.parquet",
        storage_options=storage_options,
    )

    df2 = pl.read_parquet(
        "abfss://data@dsmecdpseoul.dfs.core.windows.net/user/hive/hocean/hs4v2/H0000/dt=2/file2.parquet",
        storage_options=storage_options,
    )

    # df2 = pl.read_parquet(
    #     "abfss://data@dsmecdpseoul.dfs.core.windows.net/user/hive/dsme/ds4_pipe_pivot/H2535/IAS/ias/2026/02/11/h2535_ias_ias_20260211_pivot.parquet",
    #     storage_options=storage_options
    # )
    print("\nshow parquet")
    print(df1)
    print(df2)


if __name__ == "__main__":
    logger.info("Test Program Started")

    main()
    show_parquet()

    logger.info("Test Program Ended")

    # COMPUTE STATS 검토 필요: 테이블에 대량의 데이터를 넣은 후
    # COMPUTE STATS tmp.col_test_case2를 한 번 실행해 주세요.
    # Impala가 데이터 분포를 파악해 쿼리 실행 계획을 훨씬 효율적으로 짭니다.
