import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import logging

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


def drop_tables(cur, conn):
    """
    drop_tables drop all the tables in the Redshift cluster
    :param cur: connection cursor
    :param conn: database connection
    :return: None
    """
    for query in drop_table_queries:
        logging.info(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    create_tables create the staging tables and analytics tables in the Redshift cluster
    :param cur: connection cursor
    :param conn: database connection
    :return: None
    """
    for query in create_table_queries:
        logging.info(f"Executing query: {query}")
        cur.execute(query)
        conn.commit()


def main():
    """delete the existing tables and create new tables"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        config.get("CLUSTER", "db_host"),
        config.get("CLUSTER", "db_name"),
        config.get("CLUSTER", "db_user"),
        config.get("CLUSTER", "db_password"),
        config.get("CLUSTER", "db_port")))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()