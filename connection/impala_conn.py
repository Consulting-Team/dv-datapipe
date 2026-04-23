import atexit
import polars as pl
from config import config, logger
from impala.hiveserver2 import HiveServer2Connection
from impala.dbapi import connect

__all__ = ["ImpalaConnection"]

# logger = config.logger

class ImpalaConnection:
    def __init__(self):
        self.conn = _get_connection()

    def drop_table(self, params):
        db_name = params["db_name"]
        table_names = params["table_names"]

        conn = self.conn
        cursor = conn.cursor()

        for table_name in table_names:
            sql = f"DROP TABLE IF EXISTS {db_name}.{table_name} PURGE"
            try:
                cursor.execute(sql)
                rs = cursor.fetchall()

                for row in rs:
                    logger.debug(row[0])
            except Exception as e:
                logger.error(e)
                continue
        cursor.close()

    def create_external_table(self, params: dict, idx_access: bool = True):
        conn = self.conn
        db_name: str = params["db_name"]
        table_name: str = params["table_name"]
        schema: dict = params["schema"]
        location = params["location"]
        cols = list()
        idx_access = "true" if idx_access else "false"

        for k, v in schema.items():
            match v:
                # case pl.Float32 | pl.Float32 | pl.Float64:
                #     cols.append(f"{k} FLOAT")
                # case pl.Int8 | pl.Int16 | pl.Int32 | pl.Int64 | pl.Int128:
                #     cols.append(f"{k} INTEGER")
                # case pl.Boolean:
                #     cols.append(f"{k} BOOLEAN")
                case pl.Float32 | pl.Float32 | pl.Float64:
                    cols.append(f"{k} DOUBLE")
                case pl.Int8 | pl.Int16 | pl.Int32 | pl.Int64 | pl.Int128:
                    cols.append(f"{k} INTEGER")
                case pl.Boolean:
                    cols.append(f"{k} BOOLEAN")
                case _:
                    logger.error("지원되지 않는 데이터 타입")

        # sql문 작성
        cols = ",".join(cols)
        sql = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{table_name} ({cols})
            PARTITIONED BY (dt INTEGER)
            STORED AS PARQUET
            LOCATION '{location}'
            TBLPROPERTIES ('parquet.column.index.access'='{idx_access}')
        """
        logger.debug(f"SQL: {sql}")

        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            rs = cursor.fetchall()

            for row in rs:
                logger.info(row[0])
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            if cursor:
                cursor.close

    def add_partition(self, params: dict):
        # 파티션 추가 및 리프레시
        conn = self.conn
        db_name = params["db_name"]
        table_name = params["table_name"]
        partition_name = params["partition_name"]
        location = params["location"]

        add_sql = f"ALTER TABLE {db_name}.{table_name} ADD PARTITION ({partition_name}) LOCATION '{location}'"
        refresh_sql = f"REFRESH {db_name}.{table_name} PARTITION ({partition_name});"

        try:
            cursor = conn.cursor()
            # 파티션 추가
            cursor.execute(add_sql)
            logger.debug(f"SQL: {add_sql}")
            # rs = cursor.fetchone()
            # logger.info(rs[0])

            # 파티션 리프레시
            cursor.execute(refresh_sql)
            # logger.debug(f"SQL: {refresh_sql}")
            # rs = cursor.fetchone()

            # logger.info(rs[0])
        except Exception as e:
            logger.error(e)
            raise e
        finally:
            if cursor:
                cursor.close

    def query_polars(self, sql: str, batch_size: int = 50000, idx_access = True) -> pl.DataFrame:
        #! Impala SQL 실행 후 Polars DataFrame으로 반환
        #! 대용량의 데이터를 조회할 경우 batch_size 입력 필요
        #! batch_size: 일반적인 경우 50,000 권장
        try:
            cursor = self.conn.cursor()
            if not idx_access:
                cursor.execute("SET PARQUET_FALLBACK_SCHEMA_RESOLUTION = name")
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]

            if batch_size:
                # 대용량 데이터를 처리할 때 (배치 단위로 읽기)
                batches = list()
                while True:
                    rows = cursor.fetchmany(batch_size)

                    if not rows:
                        break

                    batch_df = pl.DataFrame(rows, schema=columns, orient="row", infer_schema_length=None)
                    batches.append(batch_df)

                if not batches:
                    return pl.DataFrame(schema=columns)

                return pl.concat(batches)
            else:
                # 일반적인 쿼리 (SHOW TABLES 등)
                rows = cursor.fetchall()
                return pl.DataFrame(
                    rows, schema=columns, orient="row", infer_schema_length=None
                )
        except Exception as e:
            logger.error(f"Impala query error: '{e}'\nSQL: {sql}")
            raise e
        finally:
            if cursor:
                cursor.close()

    def exists_db(self, db_name: str) -> bool:
        #! Impala 내 특정 데이터베이스 존재 여부 확인
        target_db = db_name.strip().lower()

        try:
            # 1. Polars DataFrame으로 DB 목록 조회
            df = self.query_polars("SHOW DATABASES;")

            # 2. Polars 식을 사용하여 존재 여부 확인
            # 'name' 컬럼에서 target_db가 있는지 검사
            is_exists = (
                df.filter(pl.col("name").str.to_lowercase() == target_db).height > 0
            )

            if is_exists:
                return True

            # logger.warning(
            #     f"The database named '{target_db}' does not exist in Impala."
            # )
            return False
        except Exception as e:
            err_msg = f"Error checking database existence: {e}"
            logger.error(err_msg)
            raise e


def _initialize() -> HiveServer2Connection:
    try:
        conn = connect(
            host=config.impala_host,
            port=config.impala_port,
            user=config.impala_user,
            password=config.impala_pwd,
            database="default",
            auth_mechanism="LDAP",
            use_ssl=True,
            use_http_transport=True,
            http_path="cliservice",
        )

        logger.debug("Successfully connected to Impala.")
        return conn

    except Exception as e:
        err_msg = "Failed to connect to Impala."
        logger.error(err_msg)
        raise e


conn: HiveServer2Connection = _initialize()


def _get_connection() -> HiveServer2Connection:
    global conn

    return conn


@atexit.register
def auto_close():
    global conn

    if conn:
        conn.close()
        logger.debug("Connection to Impala closed.")


# if __name__ == "__main__":
#     import polars as pl

#     df = pl.read_parquet("abfss://data@dsmecdpseoul.dfs.core.windows.net/user/hive/hocean/hs4v2/H0000/dt=2/file2.parquet")
#     print(df)
