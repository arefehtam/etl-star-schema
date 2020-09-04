import glob
import os

import pandas as pd
import psycopg2
from config_module import config
from sql_queries.oltp import *


def insert(cur, filepath, columns, insert_query):
    """
    insert csv file rows to corresponding database table

    Args:
        - cur: Allows to run Postgres command
        - filepath: File to be read in data frame format
        - columns: Header of file to beread
        - insert_query: SQL query command for insertion

    Returns:
    None
    """
    df = pd.read_csv(filepath, names=columns, header=None)
    for i, row in df.iterrows():
        cur.execute(insert_query, row)


def load_data(cur, conn, dirpath):
    """
    Load all files in data directory and creates Postgres tables

    Args:
        - cur: Allows to run Postgres command
        - conn: Connection to Postgres database
        - dirpath: File to be loaded and extracted to Postgres tables

    Returns:
    None
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(dirpath):
        files = glob.glob(os.path.join(root, '*.csv'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # iterate over files and process
    for filepath in all_files:
        if filepath.endswith("product.csv"):
            # insert product file
            columns = ["id", "name", "available_quantity", "location_id", "date"]
            insert(cur, filepath, columns, product_table_insert)
        elif filepath.endswith("customer.csv"):
            # insert customer file
            columns = ["id", "name", "address", "city", "date"]
            insert(cur, filepath, columns, customer_table_insert)
        elif filepath.endswith("order_info.csv"):
            # insert order_info file
            columns = ["id", "customer_id", "date"]
            insert(cur, filepath, columns, order_info_table_insert)
        elif filepath.endswith("orderline.csv"):
            # insert order_info file
            columns = ["id", "order_id", "product_id", "customer_id", "unit_price"]
            insert(cur, filepath, columns, order_line_table_insert)
        conn.commit()
        print("Done Loading " + filepath + "\n")


def main():
    """
   Load csv files to created tables of database.
    Args:
        - None

     Returns:
    None
    """
    # get jdbc config from config file
    config_object = config.resolve()
    oltp = config_object["OLTP_DB"]

    # connect to Postgress database
    conn = psycopg2.connect(
        host=oltp["host"],
        port=int(oltp["port"]),
        user=oltp["user"],
        database=oltp["database"],
        password=oltp["password"]
    )

    # create a cursor to run SQL queries
    cur = conn.cursor()

    # process order csv file
    load_data(cur, conn, dirpath=os.path.abspath("../" + config_object["APP"]["data_folder"]))

    conn.close()


if __name__ == "__main__":
    main()
