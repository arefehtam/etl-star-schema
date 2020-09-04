import psycopg2
from config_module import config


def create_database(config_object, key):
    """
    Return cursor and connection to new oltp database
    """
    admin = config_object["ADMIN_DB"]
    new_db = config_object[key]

    # connect to default database
    conn = psycopg2.connect(
        host=admin["host"],
        port=int(admin["port"]),
        user=admin["user"],
        database=admin["database"],
        password=admin["password"]
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create oltp database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS " + new_db["database"])
    cur.execute("CREATE DATABASE " + new_db["database"] + " WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to oltp database
    conn = psycopg2.connect(
        host=new_db["host"],
        port=int(new_db["port"]),
        user=new_db["user"],
        database=new_db["database"],
        password=new_db["password"]
    )
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn, drop_table_queries):
    """
    Drop each existing table from database.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn, create_table_queries):
    """
    Create tables for database.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main(config_object, key, create_table_queries, drop_table_queries):
    """
    Create database and tables based on config given
    Args:
        - config_object: contains info of all databases
        - key: a key to refer to specific database config
        - drop_table_queries: list of sql queries for drop table
        - create_table_queries: list of queries for create table

     Returns:
    None
    """
    print("Creating {} database\n".format(key))
    cur, conn = create_database(config_object, key)

    print("Creating tables\n")
    drop_tables(cur, conn, drop_table_queries)
    create_tables(cur, conn, create_table_queries)

    print("Done...\n")
    conn.close()


if __name__ == "__main__":
    # get jdbc config from config file
    from sql_queries.oltp import create_table_queries, drop_table_queries

    obj = config.resolve()
    main(obj, "OLTP_DB", create_table_queries, drop_table_queries)
