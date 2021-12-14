import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, create_schema_queries

def create_schemas(cur,conn):
    """
        Function Purpose: 
            This function executes the CREATE SCHEMA statements imported from the sql_queries.py script. 
            This function will create a staging schema (DWH.STAGING) and star schema (DWH.STAR)
            in the DWH Redshift database.
            
        Args:
            cur (psycopg2.connection): A psycopg2 PostgreSQL connection cursor.
            conn (psycopg2.connection.cursor): A psycopg2 PostgreSQL connection cursor.

        Returns:
            None
    """
    for query in create_schema_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print('Success.')

def drop_tables(cur, conn):
    """
        Function Purpose: 
            This function executes DROP TABLE statements imported from the sql_queries.py script. 
            This is done in order to ensure that the ETL proccess will insert data into empty tables
            so we first drop all the tables in the database before creating them again.
            
        Args:
            cur (psycopg2.connection): A psycopg2 PostgreSQL connection cursor.
            conn (psycopg2.connection.cursor): A psycopg2 PostgreSQL connection cursor.

        Returns:
            None
    """
    
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print('Success.')

def create_tables(cur, conn):
    """
        Function Purpose: 
            This function executes DROP TABLE statements imported from the sql_queries.py script. 
            This is done in order to ensure that the ETL proccess will insert data into empty tables
            so we first drop all the tables in the database before creating them again.
            
        Args:
            cur (psycopg2.connection): A psycopg2 PostgreSQL connection cursor.
            conn (psycopg2.connection.cursor): A psycopg2 PostgreSQL connection cursor.

        Returns:
            None
    """
    
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
        print('Success.')


def main():
    """
        Function Purpose: 
            The main function in this script will connect to the Redshift PostgreSQL data warehouse 
            and then call the drop_tables and create_tables to set up the database for ETL.
            Then this function will close the connection to the database.
        Args:
            None

        Returns:
            None
    """
    
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
