from datetime import datetime
from google.cloud import bigquery
import json
import os
from pathlib import Path
from tqdm import tqdm


class Importer():
    def __init__(self, gcloud_credentials, gcloud_project, query=None, results=None):
        '''set object data and connect with the service'''
        self.query = query
        self.results_fname = results

        # todo: find out if better way of setting the credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcloud_credentials
        os.environ['GCLOUD_PROJECT'] = gcloud_project
        
        self._connect()

    def _connect(self):
        '''connect with BigQuery'''
        self.client = bigquery.Client()

    def _execute(self, query=None):
        '''execute the SQL and save to a dataframe'''
        if query:
            self.query = query
        if not self.query:
            raise Exception("Query must be specified")
        print("executing SQL query:", self.query)
        query_job = self.client.query(self.query)
        self.df = query_job.to_dataframe(progress_bar_type='tqdm')
        print("query got", len(self.df), "rows")

    def _transform(self):
        '''transform the dataframe into the specified object structure, then serialize into a JSON string
        {
            date_of_execution: <date>, 
            query: <query that was passed in>,
            records: [
                {id: 0, label: <ground truth label>, data: <whatever json results, can be any structure as long as consistent across rows>},
                {id: 1, label: <ground truth label>, data: <see above>},
                ...
            ]
        }
        '''

        new_object = {
            "date_of_execution": datetime.utcnow().timestamp(),
            "query": self.query, 
            "records": [{ "id": i, "label": self._serialize_keys(row), "data": self._serialize_values(row) } for i,row in tqdm(self.df.iterrows(), total=len(self.df))]
        }

        return json.dumps(new_object)

    def _serialize_keys(self, row):
        return list(row.index)

    def _serialize_values(self, row):
        '''rely on pandas to serialize all datatypes that BigQuery returns (eg Decimal, date, etc)'''
        return list(json.loads(row.to_json()).values())

    def run(self, query=None, results=None):
        '''function for executing the given SQL query and returning the transformed data'''
        self._execute(self.query)
        j = self._transform()

        if results:
            self.results_fname = results
        if not self.results_fname:
            self.results_fname = "results.json"

        with open(self.results_fname, 'w') as f:
            f.write(j)
        return Path(self.results_fname)

if __name__=="__main__":
    print("Running importer")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='the location of the config file', required=True)

    args = parser.parse_args()

    with open(args.config) as f:
        config = json.loads(f.read())
    imp = Importer(**config)
    path = imp.run()
    print("Results at", path)
