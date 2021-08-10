import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 into Refshift staging tables.

    Args:
        cur: database cursor.
        conn: database connection.

    Returns: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Loads data from staging tables into fact and dimension tables. The data
    is transformed, i.e. cleaned and deduped for the purpose of creating high
    quality facts and dimensions.

    Args:
        cur: database cursor.
        conn: database connection.

    Returns: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    ETL process entrypoint. First connection to the database is established,
    then data is loaded into staging tables, and finally data is transformed
    and loaded into fact and dimension tables.

    Args: None

    Returns: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()