import os
from tqdm import tqdm

class Client():
    '''instantiates a generic Client object with a query method'''
    def __init__(self, database_type, credentials):
        self.type = database_type
        if database_type == 'bigquery':
            from google.cloud import bigquery
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials['gcloud_credentials']
            os.environ['GCLOUD_PROJECT'] = credentials['gcloud_project']
            self.client = bigquery.Client()

        else:
            raise Exception(f"Database type '{database_type}' not supported")

    def query(self, sql):
        '''queries the database and returns a dataframe'''
        query_job = self.client.query(sql)
        return query_job.to_dataframe(progress_bar_type='tqdm')
