from dataclasses import dataclass
from datetime import date
from jmlogger import get_logger
from dotenv import load_dotenv
from logging import Logger
from pathlib import Path
import os
import sys
import argparse

# 워킹 디렉토리 설정
base_path = (
    Path(sys.executable).parent
    if getattr(sys, "frozen", False)
    else Path(__file__).parent
)
os.chdir(base_path)


@dataclass
class Config:
    hull: str
    start: date
    end: date
    # date: date
    logger: Logger
    storage_location: str
    clear: bool

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
        self.logger.info("🔔 Configurations")
        self.logger.info(f"   - Hull Number: {self.hull}")
        # self.logger.info(f"   - Input Date: {self.date}")
        self.logger.info(f"   - Start Date: {self.start}")
        self.logger.info(f"   - End Date: {self.end}")
        self.logger.info(f"   - Impala Host: {self.impala_host}")
        self.logger.info(f"   - Impala Port: {self.impala_port}")
        self.logger.info(f"   - Impala User: {self.impala_user}")
        self.logger.info(f"   - Storage Account: {self.hs4v1_abfs_strg_acc}")
        self.logger.info(f"   - Container Name: {self.hs4v1_abfs_strg_cont}")
        self.logger.info(f"   - Clear Option: {self.clear}")

    def get_storage_options(self) -> dict[str, str]:
        """오브젝트 스토리지 접근을 위한 정보 반환"""

        return {
            "account_name": config.hs4v1_abfs_strg_acc,
            "account_key": config.hs4v1_abfs_strg_key
        }


def _initiaize() -> Config:
    # global config, logger

    # .env 정보 읽기
    load_dotenv(override=False)

    # arguments 파싱
    args = _parse_args()

    # logger 이름
    logger = get_logger(f"logs/{args.hull}_datapipe_iceberg.log")

    # Ojbect sotrage 위치
    hull = args.hull
    strg_container_name = os.getenv("HS4V1_ABFS_STRG_CONT")
    strg_account_name = os.getenv("HS4V1_ABFS_STRG_ACC")
    strg_location = f"abfss://{strg_container_name}@{strg_account_name}.dfs.core.windows.net/user/hive/hocean/hs4v2/{hull}/"

    # config object 구성
    config = Config(
        hull=hull,
        # date=args.date,
        start=args.start,
        end=args.end,
        hs4v1_abfs_strg_acc=strg_account_name,
        hs4v1_abfs_strg_key=os.getenv("HS4V1_ABFS_STRG_KEY"),
        hs4v1_abfs_strg_protocol=os.getenv("HS4V1_ABFS_STRG_PROTOCOL"),
        hs4v1_abfs_strg_cont=strg_container_name,
        impala_host=os.getenv("IMPALA_HOST"),
        impala_port=os.getenv("IMPALA_PORT"),
        impala_user=os.getenv("IMPALA_USER"),
        impala_pwd=os.getenv("IMPALA_PWD"),
        logger=logger,
        storage_location=strg_location,
        clear=args.clear
    )

    return config


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="Data Collection", description="Data Pipie for Data Visualization."
    )
    parser.add_argument(
        "--hull", type=str, default="H0000", help="input hull number (H0000)"
    )
    # parser.add_argument(
    #     "--date",
    #     type=date.fromisoformat,
    #     default=str(date.today()),
    #     help="input date in YYYY-MM-DD fromat",
    # )
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=str(date.today()),
        help="start date in YYYY-MM-DD fromat",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=str(date.today()),
        help="end date in YYYY-MM-DD fromat",
    )
    parser.add_argument("-c", "--clear", action='store_true', help="")
    args = parser.parse_args()

    # hull number 'H'로 시작하는지 체그
    hnum: str = args.hull
    args.hull = hnum.strip().upper()

    return args



config: Config = _initiaize()
logger = config.logger

if __name__ == "__main__":
    _initiaize()
