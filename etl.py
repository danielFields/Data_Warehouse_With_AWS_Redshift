import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query.split('\n')[1])
        cur.execute(query)
        conn.commit()
        print('Success.')

def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query.split('\n')[1])
        cur.execute(query)
        conn.commit()
        print('Success.')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connected to Database.')
    
    print('Loading Data from S3 to Stage Tables')
    load_staging_tables(cur, conn)
    
    print('Inserting Data from OLTP Staging Tables into OLAP Star-Schema Tables.')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()