"""
메타데이터 업로드 코드
"""

import os
import argparse
import polars as pl
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from dotenv import load_dotenv

SCHEMA = "data_visualization"


def get_hnum(df: pl.DataFrame) -> str:
    # 데이터프레임에서 호선 번호 추출 후 반환
    # 호선 번호가 없거나 유니크하지 않을 경우 None 반환
    hnums = df["hnum"].unique().to_list()

    if len(hnums) != 1:
        raise ValueError("Hull number is not unique.")

    return hnums[0].upper()


def main():
    # 환경변수 읽기 (.env)
    load_dotenv(override=False)

    # --path 인자로부터 csv 경로 설정
    path = get_meta_path()

    # 메타데이터 읽어오기
    if path.exists():
        df = pl.read_csv(path)
        hnum = get_hnum(df)
    else:
        print(f"File not found: {str(path.absolute())}")

    try:
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            database=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PWD"),
        )
        cursor = conn.cursor()

        data_to_insert = df.rows()
        cols = df.columns
        columns = ", ".join(cols)
        update_cols = [col for col in cols if col not in ["hnum", "col_name"]]
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])

        # 메타데이터 리스트에서 배제된 태그 데이터베이스에서 삭제
        sql = f"""
            SELECT id, col_name
            FROM {SCHEMA}.metadata
            WHERE col_name NOT IN ({', \n'.join([f"'{col}'" for col in df['col_name'].to_list()])})
            AND hnum = '{hnum}';
        """
        cursor.execute(sql)

        remove_list = [id for (id, _) in cursor.fetchall()]
        if remove_list:
            sql = f"DELETE FROM {SCHEMA}.metadata WHERE id IN %s"
            cursor.execute(sql, (tuple(remove_list),))

        # 데이터 삽입 / 업데이트
        sql = f"""
            INSERT INTO {SCHEMA}.metadata ({columns})
            VALUES %s
            ON CONFLICT (hnum, col_name)
            DO UPDATE SET
                {update_set}
        """
        execute_values(cursor, sql, data_to_insert)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(e)
        conn.rollback()
        raise ValueError(e)

    print(f"Metadata for '{hnum}' has been updated.")

    return


def get_meta_path() -> Path:
    parser = argparse.ArgumentParser(
        prog="Upload Metadata", description="Upload the metadata. (csv format)"
    )
    parser.add_argument("--path", type=str, help="Path to the metadata file.")
    args = parser.parse_args()

    return Path(args.path)


if __name__ == "__main__":
    main()
