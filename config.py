from dataclasses import dataclass
from datetime import date
from jmlogger import get_logger
from dotenv import load_dotenv
from logging import Logger
import os
import argparse

@dataclass
class Config:
    hull: str
    date: date
    logger: Logger

    # impala 연결정보
    impala_host: str | None
    impala_port: str | None
    impala_user: str | None
    impala_pwd: str | None

    # abfs 연결정보
    hs4v1_abfs_strg_acc: str | None
    hs4v1_abfs_strg_key: str | None
    hs4v1_abfs_strg_protocol: str | None
    hs4v1_abfs_strg_cont: str | None

    def display(self):
        logger.info("🔔 Configurations")
        logger.info(f"   - Hull Number: {self.hull}")
        logger.info(f"   - Input Date: {self.date}")
        logger.info(f"   - Impala Host: {self.impala_host}")
        logger.info(f"   - Impala Port: {self.impala_port}")
        logger.info(f"   - Impala User: {self.impala_user}")
        logger.info(f"   - Storage Account: {self.hs4v1_abfs_strg_acc}")
        logger.info(f"   - Container Name: {self.hs4v1_abfs_strg_cont}")

        return


def _initiaize() -> Config:
    global config, logger

    # .env 정보 읽기
    load_dotenv(override=False)

    # arguments 파싱
    args = _parse_args()

    # logger 이름
    logger = get_logger(f"logs/{args.hull}_iceberg_test.log")

    # config object 구성
    config = Config(
        hull=args.hull,
        date=args.date,
        hs4v1_abfs_strg_acc=os.getenv("HS4V1_ABFS_STRG_ACC"),
        hs4v1_abfs_strg_key=os.getenv("HS4V1_ABFS_STRG_KEY"),
        hs4v1_abfs_strg_protocol=os.getenv("HS4V1_ABFS_STRG_PROTOCOL"),
        hs4v1_abfs_strg_cont=os.getenv("HS4V1_ABFS_STRG_CONT"),
        impala_host=os.getenv("IMPALA_HOST"),
        impala_port=os.getenv("IMPALA_PORT"),
        impala_user=os.getenv("IMPALA_USER"),
        impala_pwd=os.getenv("IMPALA_PWD"),
        logger=logger,
    )

    return config


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="Data Collection", description="Data Pipie for Data Visualization."
    )

    try:
        parser.add_argument(
            "--hull", type=str, default="H0000", help="input hull number (H0000)"
        )
        parser.add_argument(
            "--date",
            type=date.fromisoformat,
            default=str(date.today()),
            help="input date in YYYY-MM-DD fromat",
        )
        args = parser.parse_args()

        # hull number 'H'로 시작하는지 체그
        hnum: str = args.hull
        args.hull = hnum.strip().upper()

        return args
    except Exception as e:
        logger.error(e)


config: Config = _initiaize()

if __name__ == "__main__":
    _initiaize()
