import psycopg2
from config_module import config
from create_tables import main as create_tables
from sql_queries.dwh import *
from sql_queries.oltp import *


def from_to(cur_read, cur_write, select_query, insert_query):
    """
    read data from source database via select_query and write to target db via insert_query
    Args:
        - cur_read: cursor to source db
        - cur_write: cursor to target db
        - select_query: sql query to select source table
        - insert_query: sql query to insert to target table
     Returns:
    None
    """
    cur_read.execute(select_query)
    rows = cur_read.fetchall()

    for row in rows:
        cur_write.execute(insert_query, row)


def transfer_data(config_object):
    """"
    Transfer data from oltp db to dwh db using two database connection
    Args:
        - config_object: contains info of all databases
     Returns:
    None
    """
    conn_oltp = ""
    conn_dwh = ""
    try:
        oltp = config_object["OLTP_DB"]
        dwh = config_object["DWH_DB"]
        conn_oltp = psycopg2.connect(
            host=oltp["host"],
            port=int(oltp["port"]),
            user=oltp["user"],
            database=oltp["database"],
            password=oltp["password"]
        )
        conn_dwh = psycopg2.connect(
            host=dwh["host"],
            port=int(dwh["port"]),
            user=dwh["user"],
            database=dwh["database"],
            password=dwh["password"]
        )
        conn_oltp.autocommit = True
        conn_dwh.autocommit = True

    except:
        print("Unable to connect to the database.")

    cursor_oltp = conn_oltp.cursor()

    cursor_dwh = conn_dwh.cursor()

    # transfer product
    from_to(cursor_oltp, cursor_dwh, product_table_transfer, dim_product_table_insert)
    print("Transfered dimension: product table\n" + dim_product_table_insert)

    # transfer product_location
    from_to(cursor_oltp, cursor_dwh, product_location_table_transfer, product_location_table_insert)
    print("Transfered dimension: product_location table\n" + product_location_table_insert)

    # transfer customer_profile
    from_to(cursor_oltp, cursor_dwh, customer_table_transfer, dim_customer_profile_table_insert)
    print("Transfered dimension: customer_profile table\n" + dim_customer_profile_table_insert)

    # transfer order date
    from_to(cursor_oltp, cursor_dwh, order_date_transfer, dim_order_date_table_insert)
    print("Transfered dimension: order_date table\n" + dim_order_date_table_insert)

    # transfer order
    from_to(cursor_oltp, cursor_dwh, order_table_transfer, fact_order_table_insert)
    print("Transfered fact: order table\n" + fact_order_table_insert)

    conn_dwh.commit()

    conn_oltp.close()

    conn_dwh.close()


def main():
    """"
    This first create target database and tables, then transfer data from source to target
    Args:
        - None
     Returns:
    None
    """
    # get jdbc config
    config_object = config.resolve()

    # create owh db databases and tables
    create_tables(config_object, "DWH_DB", dim_fact_create_table_queries, dim_fact_drop_table_queries)

    # transfer from oltp to dwh db
    transfer_data(config_object)


if __name__ == "__main__":
    main()
