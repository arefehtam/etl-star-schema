import psycopg2
from config_module import config


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

    # connect to Postgress database
    dwh = config_object["DWH_DB"]
    conn = psycopg2.connect(
        host=dwh["host"],
        port=int(dwh["port"]),
        user=dwh["user"],
        database=dwh["database"],
        password=dwh["password"]
    )
    cur = conn.cursor()

    # questions
    # 1. What is the average order price per customer
    print("1. What is the average order price per customer\n")
    sql_query = """SELECT customer_id, avg(total_price) FROM fact_order GROUP BY customer_id;"""
    print(sql_query)
    cur.execute(sql_query)
    res = [str(item[0]) + ":" + str(item[1]) for item in cur.fetchall()]
    print("Result:\n")
    print(res)

    # 2. How many ‘Vacuum cleaners’ were ordered in New York? Ans who bought most of them in this city?
    print(" 2. How many ‘Vacuum cleaners’ were ordered in New York?\n")
    sql_query = """SELECT SUM(quantity) FROM fact_order 
                   WHERE city = 'New York' AND product_id = (SELECT id FROM dim_product WHERE name = 'Vacuum cleaner')
                """
    print(sql_query)
    cur.execute(sql_query)
    res = [item[0] for item in cur.fetchall()]
    print("Result:\n")
    print(res)

    print(" Ans who bought most of them in this city?\n")
    sql_query = """
    WITH tbl1 AS  (
        SELECT customer_id, SUM(quantity) AS sum_quantity FROM fact_order 
            WHERE city = 'New York' AND product_id = (SELECT id FROM dim_product WHERE name = 'Vacuum cleaner')
            GROUP BY customer_id
        )
    SELECT name FROM dim_customer_profile 
    WHERE customer_id = (SELECT customer_id FROM tbl1 WHERE sum_quantity = (SELECT MAX(sum_quantity) FROM tbl1))
    FETCH FIRST 1 ROWS ONLY;
    """
    print(sql_query)
    cur.execute(sql_query)
    res = [item[0] for item in cur.fetchall()]
    print("Result:\n")
    print(res)

    print("3.What product is the most popular in each city?")
    # Note that popularity means maximum total cost
    sql_query = """
    WITH total_cost_per_product_per_city AS (
        SELECT product_id , city, SUM(total_price) AS sum_total_price FROM fact_order GROUP BY product_id, city
    )
    SELECT dp.name, total_cost_per_product_per_city.city, sum_total_price FROM total_cost_per_product_per_city 
    INNER JOIN (
        SELECT city, MAX(sum_total_price) AS max_total_price FROM total_cost_per_product_per_city GROUP BY city
    ) c ON (total_cost_per_product_per_city.city = c.city AND total_cost_per_product_per_city.sum_total_price = max_total_price)
    INNER JOIN dim_product dp on dp.id = total_cost_per_product_per_city.product_id
    """
    print(sql_query)
    cur.execute(sql_query)
    res = [item for item in cur.fetchall()]
    print("Result:\n")
    print(res)


if __name__ == "__main__":
    main()
