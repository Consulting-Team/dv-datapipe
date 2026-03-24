import csv
import polars as pl
from config import logger
from dataclasses import dataclass
# from jmlogger import logger

# logger = config.logger

@dataclass
class MetaData:
    col_name: str
    tag: str
    description: str
    unit: str
    data_type: str
    table_name: str
    db_name: str


### CSV 파일을 읽어 column 메타정보 반환
def read_metadata(path: str) -> dict[str, MetaData]:
    logger.info(f"👉 Read the meatadata from '{path}'.")
    keywords = ["cams", "ias", "ams", "vdr"]

    df = pl.read_csv(path)

    tables = (
        df.select("table_name")
        .unique()
        .sort(
            by=[pl.col("table_name").str.contains(k) for k in keywords]
            + [pl.col("table_name")],
            descending=[True] * len(keywords) + [False],
        )
        .get_column("table_name")
        .to_list()
    )

    tables = {name: [] for name in tables}

    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            info = MetaData(**row)
            tables[info.table_name].append(info)

    return tables


def get_db_name(metadata_dict: dict[str, MetaData]) -> str | None:
    db_names = {
        m.db_name for metadata_list in metadata_dict.values() for m in metadata_list
    }

    if len(db_names) == 1:
        return db_names.pop()
    else:
        err_msg = f"DB must be unique. (fount: {', ' if db_names else 'None'})"
        logger.error(err_msg)
        raise ValueError(err_msg)


# if __name__ == "__main__":
#     path = "resources/csv/H2521_metadata.csv"
#     read_metadata(path)
