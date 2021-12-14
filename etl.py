import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):    
    """
        Function Purpose: 
            This function executes the COPY INTO statements imported from the sql_queries.py script. 
            This function will call the copy into commands to load data from S3 bucket to
            our Redshift cluster.
            
        Args:
            cur (psycopg2.connection): A psycopg2 PostgreSQL connection cursor.
            conn (psycopg2.connection.cursor): A psycopg2 PostgreSQL connection cursor.

        Returns:
            None
    """
    for query in copy_table_queries:
        print(query.split('\n')[1])
        cur.execute(query)
        conn.commit()
        print('Success.')
        
        
        
        

def insert_tables(cur, conn):
    """
        Function Purpose: 
            This function executes the INSERT INTO statements imported from the sql_queries.py script. 
            This function will call the insert into commands to query data from the staging tables
            and then insert the results of that query into the fact table and dimension tables.
            
        Args:
            cur (psycopg2.connection): A psycopg2 PostgreSQL connection cursor.
            conn (psycopg2.connection.cursor): A psycopg2 PostgreSQL connection cursor.

        Returns:
            None
    """
    for query in insert_table_queries:
        print(query.split('\n')[1])
        cur.execute(query)
        conn.commit()
        print('Success.')

        
        

def main():
    """
        Function Purpose: 
            The main function in this script will connect to the Redshift PostgreSQL data warehouse 
            and then call the SQL commands to do ETL from S3 to Redshift (E) 
            then transform the extracted data by querying the extracted data 
            then load the data into dimension and fact tables.
            Then this function will then close the connection to the database.
        Args:
            None

        Returns:
            None.
    """
    
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
