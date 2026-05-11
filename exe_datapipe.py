"""
데이터가시화 과제용 데이터파이프 코드
iceberg 테이블을 생성하고 기존 테이블에서 데이터를 복사해서 삽입
"""

from config import config, logger
from time import perf_counter
from utils import metadata, schema_manager as sm


def main():
    # configuration 출력
    config.display()

    # 시작 / 끝 날짜
    start = config.start.strftime("%Y%m%d")
    end = config.end.strftime("%Y%m%d")

    # 메타데이터 읽기
    metadata_path = config.metapath
    metadata_dict = metadata.read_metadata(metadata_path)
    hulls_in_metadata = {table.split("_")[0].lower() for table in metadata_dict.keys()}

    # 호선 번호 체크
    if config.hull.lower() not in hulls_in_metadata or len(hulls_in_metadata) > 1:
        print(f"Warning: Hull number mismatch!")
        print(f" - Config: {config.hull}")
        print(f" - Metadata found: {', '.join(hulls_in_metadata)}")

        confirm = input("Will you proceed the process? [y/n]: ").lower()
    if confirm != "y":
        print("Process aborted.")
        exit()  # 또는 return

    if config.clear:
        # 기존 테이블 및 데이터 삭제
        sm.clear_table()

    # Iceberg 테이블 생성
    col_names = sm.create_iceberg_table(metadata_dict)

    # 데이터 삽입
    sm.insert_data(col_names, metadata_dict, start=start, end=end)


if __name__ == "__main__":
    start = perf_counter()
    sep = "-" * 80
    logger.info(f"{sep}")

    main()

    logger.info(f"👀 Elapsed time: {perf_counter() - start: .2f} (sec)")
    logger.info(f"{sep}")
