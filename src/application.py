""" Main Module"""
from config_module import config
from create_tables import main as create_tables
from etl import main as etl
from load_csv_files import main as load_csv_file
from question import main as question
from sql_queries.oltp import create_table_queries, drop_table_queries

"""
 3 thins done in this project:
 1- Creating original database and tables
 2- Loading csv files data to original table
 3- Creating new database and tables which is a fact and multiple dimension tables through sql
 queries for transformation in etl process. Queries can be check in sql_queries.oltp module
"""

if __name__ == '__main__':
    obj = config.resolve()
    create_tables(obj, "OLTP_DB", create_table_queries, drop_table_queries)
    load_csv_file()
    etl()
    question()
