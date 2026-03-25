import sys
import unittest
from dv_datapipe import get_schema, read_metadata, query_data
from config import config

class MyUnitTest(unittest.TestCase):

    def test_get_schema(self):
        metadata_dict = read_metadata(f"resources/csv/{config.hull}_metadata.csv")
        schema = get_schema(metadata_dict)
        df = query_data(metadata_dict, start="20260324", end="20260324")

        print(df)

        # for k, v in schema.items():
        #     print(f"{k}: {v}")

if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])