# 오브젝트 스토리지에 저장된 parquet 파일 읽기
from config import config
import polars as pl


def get_df_from_obj_strg(location: str) -> pl.DataFrame:
    df = pl.read_parquet(location, storage_options=config.get_storage_options())

    return df


if __name__ == "__main__":
    container = config.hs4v1_abfs_strg_cont
    account = config.hs4v1_abfs_strg_acc

    location = (
        f"abfss://{container}@{account}.dfs.core.windows.net/"
        f"user/hive/dsme/ds4_pipe_pivot/H2521/VDR/aivdo1/2026/03/30/"
        f"h2521_vdr_aivdo1_20260330_pivot.parquet"
    )
    df = get_df_from_obj_strg(location)

    print(df)
