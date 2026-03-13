from jmlogger import logger
from connection import ImpalaConnection

def main():
    logger.info("Program started")

    conn = ImpalaConnection()
    df = conn.query_polars("SHOW TABLES IN insw;")
    print(df)


if __name__ == "__main__":
    main()