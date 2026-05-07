from config import config

# 테이블이 생성될 DB
DB_NAME = "dv"
# 새로 생성할 테이블 이름
TABLE_NAME = f"{config.hull}_raw"
# m/s에서 knot로 단위 변환 상수
MS_TO_KN = 900.0 / 463.0