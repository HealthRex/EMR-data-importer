import os
from tqdm import tqdm

class Client():
    '''instantiates a generic Client object with a query method'''
    def __init__(self, database_type, credentials=None, filename=None):
        self.type = database_type.lower()
        if self.type == 'bigquery':
            from google.cloud import bigquery
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials['gcloud_credentials']
            os.environ['GCLOUD_PROJECT'] = credentials['gcloud_project']
            self.client = bigquery.Client()
        elif self.type == 'sqlite':
            import sqlite3
            if not filename:
                raise Exception("filename must be defined for sqlite type")
            self.con = sqlite3.connect(filename)
        elif self.type == 'csv':
            if not filename:
                raise Exception("filename must be defined for sqlite type")
            self.fname = filename
            from pandas import read_csv
            self.df = read_csv(filename)
        elif self.type == 'dummy':
            print("running dummy database client")
            self.client = None
        else:
            raise Exception(f"Database type '{database_type}' not supported")

    def query(self, sql=None):
        '''queries the database and returns a dataframe'''
        if self.type == 'bigquery':
            query_job = self.client.query(sql)
            return query_job.to_dataframe(progress_bar_type='tqdm')
        elif self.type == 'sqlite':
            from pandas import read_sql_query
            return read_sql_query(sql, self.con)
        elif self.type == 'csv':
            if not sql:
                return self.df
            # run the query by dumping to an in-memory SQL table and running on that
            import sqlite3
            from pandas import read_sql_query
            con = sqlite3.connect(":memory:")
            self.df.to_sql(self.fname.split('.csv')[0], con)
            return read_sql_query(sql, con)
        elif self.type == 'dummy':
            from pandas import DataFrame
            return DataFrame()