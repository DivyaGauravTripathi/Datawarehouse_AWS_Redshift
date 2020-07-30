import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('------------------')
        print('Processing query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('------------------')
        print('{} processed OK.'.format(query))
    print('All files COPIED OK.')


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('------------------')
        print('Processing query: {}'.format(query))
        cur.execute(query)
        conn.commit()
        print('------------------')
        print('{} processed OK.'.format(query))
    print('All files INSERTED OK.')



def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("AWS Redshift connection established OK.")
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()