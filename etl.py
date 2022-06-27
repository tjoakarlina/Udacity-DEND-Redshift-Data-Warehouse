import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import logging

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

def load_staging_tables(cur, conn):
    """
    load_staging_tables load the raw data from S3 and dump the data into staging tables
    :param cur: database connection cursor
    :param conn: database connection
    :return: None
    """
    logging.info("Load the data to the staging tables")
    for query in copy_table_queries:
        logging.info(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    insert_tables transform data from the staging tables to analytics tables
    :param cur: database connection cursor
    :param conn: database connection
    :return: None
    """
    logging.info("load the data to the analytics tables")
    for query in insert_table_queries:
        logging.info(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()


def main():
    """Load data from S3 server to analytics tables"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        config.get("CLUSTER", "db_host"),
        config.get("CLUSTER", "db_name"),
        config.get("CLUSTER", "db_user"),
        config.get("CLUSTER", "db_password"),
        config.get("CLUSTER", "db_port")))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()