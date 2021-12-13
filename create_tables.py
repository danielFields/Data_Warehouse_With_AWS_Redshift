import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_queries

def create_schemas(cur,conn):
    """This will create the staging and star schemas in the redshift cluster if the schemas do not already exist."""
    for query in create_schema_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print('Success.')

def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print('Success.')

def create_tables(cur, conn):
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print('Success.')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    #Set autocommit to true so we can do DDL in sequence
    conn.autocommit = True
    print("Connected.")
    cur = conn.cursor()
    
    print("Creating Schemas.")
    create_schemas(cur,conn)
    
    print("Dropping old tables.")
    drop_tables(cur, conn)
    
    print("Creating New Tables.")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()