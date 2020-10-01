from datetime import datetime
import importlib
import json
import os
from pathlib import Path
from tqdm import tqdm

from .db_client import Client

class Importer():
    def __init__(self, database_type, credentials, label_extractor=None, client=None, query=None, results=None):
        '''set object data and connect with the service'''
        self.query = query
        self.results_fname = results
        
        if not client:
            client = Client(database_type, credentials)
        self.client = client

        # allows the user to specify a script to get the ground truth
        if label_extractor is not None:
            self.get_row_label = importlib.import_module(label_extractor).get_row_label
        else:
            self.get_row_label = self._make_indices_list

    def _execute(self, query=None):
        '''execute the SQL and save to a dataframe'''
        if query:
            self.query = query
        if not self.query:
            raise Exception("Query must be specified")
        print("executing SQL query:", self.query)
        self.df = self.client.query(self.query)
        print("query got", len(self.df), "rows")

    def _transform(self):

        '''transform the dataframe into a structured object, then serialize into a JSON string'''
        new_object = {
            "date_of_execution": datetime.utcnow().timestamp(),
            "query": self.query, 
            "records": [{ "id": i, "label": self.get_row_label(row), "data": self._serialize_values(row) } for i,row in tqdm(self.df.iterrows(), total=len(self.df))]
        }
        return json.dumps(new_object)

    def _make_indices_list(self, row):
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
