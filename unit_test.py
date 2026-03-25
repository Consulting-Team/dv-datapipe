import sys
import unittest
from utils.metadata import get_schema
from dv_datapipe import read_metadata, query_data
from config import config


class MyUnitTest(unittest.TestCase):

    def test_get_schema(self):
        """메타데이터 기반 스키마 생성 및 데이터 타입 변환 검증"""

        # 1. 메타데이터 읽기
        metadata_path = f"resources/csv/{config.hull}_metadata.csv"
        metadata_dict = read_metadata(metadata_path)

        # 2. 스키마 생성
        schema = get_schema(metadata_dict)
        self.assertIsInstance(schema, dict, "The data type for schema must be dict.")

        # 3. 데이터 쿼리
        df = query_data(metadata_dict, start="20260324", end="20260324")

        # 4. 검증
        if not df.is_empty():
            # 열 이름이 스키마와 일치하는지 확인
            self.assertTrue(
                set(schema.keys()).issubset(set(df.columns)),
                "Some columns defined in metadata are missing in the query result.",
            )

            # 실제 적용된 데이터 타입이 스키마와 일치하는지 확인
            for col, dtype in schema.items():
                if col in df.columns:
                    self.assertEqual(
                        df.schema[col], dtype, f"Data type mismatch for column: {col}"
                    )

        else:
            self.skipTest("No data found for the test period. Skipping assertion.")


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
