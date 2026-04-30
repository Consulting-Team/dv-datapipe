from config import config, logger
from utils.metadata import *

def main():
    config.display()

    logger.info("hello world")

    # 추가할 열 메타데이터 읽기
    metadata_list = read_metadata(f"resources/csv/{config.hull}_append_columns.csv")
    for table, mds in metadata_list.items():
        for md in mds:
            print(md)

    # raw 테이블 열 불러오기

    # 메타데이터와 raw 테이블 열 비교

    # 데이터 삽입
    start = config.start.strftime("%Y%m%d")
    end = config.end.strftime("%Y%m%d")

    return

if __name__ == "__main__":
    main()