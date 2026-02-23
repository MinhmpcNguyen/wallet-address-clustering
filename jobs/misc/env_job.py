# This file is to test importing on Airflow cluster
# import sys
# import os
# sys.path.append(os.path.dirname(os.path.dirname(sys.path[0])))
from config import ArangoDBConfig


def print_database():
    print(f"address-clustering-etl Arango Database: {ArangoDBConfig.DATABASE}")
    # print(f"Pandas: {pd.__version__}")


if __name__ == '__main__':
    print_database()
