from emr_importer import Importer, open_config
from db_client import Client
import json
import os
import unittest

class BigqueryTest(unittest.TestCase):
    def setUp(self):
        with open("test_config.json") as f:
            self.config = json.loads(f.read())
        self.importer = Importer(database_type="bigquery", credentials={
            "gcloud_credentials": self.config["gcloud_credentials"], 
            "gcloud_project": self.config["gcloud_project"]
        })

    def test_config_file_exists(self):
        self.assertTrue(os.path.exists('config.json') or os.path.exists('configs/config.json'))

    def test_open_config(self):
        open_config()

class TestDataTranformation(BigqueryTest):
    '''Given data in the form of a database response, test that it transforms into the expected format'''
    # to test: data in valid format? data in readable json after run() completed?
    def test_label_extractor_test(self):
        importer = Importer(database_type="bigquery", label_extractor="label_extractor", credentials={
            "gcloud_credentials": self.config["gcloud_credentials"], 
            "gcloud_project": self.config["gcloud_project"]
        })

class TestRun(BigqueryTest):
    def test_run(self):
        db = self.config["databases"][0]
        table = self.config["tables"][0]
        query = f'SELECT * FROM {db}.{table} LIMIT 1;'
        print(f"query: '{query}'")
        self.importer.run(query)

class TestClientInstantiation(unittest.TestCase):
    def test_csv(self):
        with self.assertRaises(Exception):
            client = Client('csv')
        client = Client('csv', filename='test.csv')

    def test_sqlite(self):
        with self.assertRaises(Exception):
            client = Client('sqlite')
        client = Client('sqlite', filename='test.sqlite')

    def test_dummy(self):
        client = Client('dummy')

    def test_nonexistant(self):
        with self.assertRaises(Exception):
            client = Client("dne")

class TestClientQuery(unittest.TestCase):
    def setUp(self):
        self.dummy_client = Client('dummy', filename='test.csv')
        self.csv_client = Client('csv', filename='test.csv')
        self.sqlite_client = Client('sqlite', filename='test.sqlite')

    def test_csv(self):
        self.csv_client.query()
        self.csv_client.query("SELECT * FROM test LIMIT 1;")

    def test_sqlite(self):
        self.sqlite_client.query("SELECT * FROM test LIMIT 1;")

    def test_dummy(self):
        self.dummy_client.query()
        self.dummy_client.query("SELECT * FROM test LIMIT 1;")

if __name__=="__main__":
    unittest.main()