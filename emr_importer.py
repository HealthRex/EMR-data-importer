from datetime import datetime
import importlib
import json
import os
from pathlib import Path
from tqdm import tqdm

from db_client import Client

class Importer():
    def __init__(self, database_type, credentials, label_extractor=None, client=None, query=None, results=None):
        '''set object data and connect with the service'''
        self.query = query
        if results:
            self.results_fname = results
        else:
            self.results_fname = 'results.json'    
        if not client:
            client = Client(database_type, credentials)
        self.client = client

        # allows the user to specify a script to get the ground truth
        if label_extractor is not None:
            self.get_row_label = importlib.import_module(label_extractor).get_row_label
            from pandas import Series
            try:
                json.dumps(self.get_row_label(Series([1,2,3])))
            except Exception as e:
                raise Exception("label extractor must handle input type of pandas.Series and output a json-serializable object")
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

    def run(self, query=None):
        '''function for executing the given SQL query and returning the transformed data'''
        if query:
            self.query = query
        self._execute(self.query)
        j = self._transform()

        with open(self.results_fname, 'w') as f:
            f.write(j)
        return Path(self.results_fname)

def open_config(args=None):
    try:
        with open(args.config) as f:
            config = json.loads(f.read())
            print("loaded", args.config)
    except:
        try:
            with open(os.path.join(os.path.dirname(__file__),"config.json"), 'r') as f:
                config = json.loads(f.read())
                print("loaded", os.path.join(os.path.dirname(__file__),"config.json"))
        except Exception as e:
                raise Exception("No config file found in package directory or specified on command line") from e
    if 'credentials' not in config:
        raise Exception("'credentials' parameter required in config file")

if __name__=="__main__":
    print("Running importer")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='the location of the config file', required=True)

    args = parser.parse_args()
    config = open_config(args)

    imp = Importer(**config)
    path = imp.run()
    print("Results at", path)
