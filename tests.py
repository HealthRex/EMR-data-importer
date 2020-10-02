from emr_importer import Importer, open_config
import json
import os
import unittest

class DataImporterTest(unittest.TestCase):
    def setUp(self):
        with open("test-config.json") as f:
            self.config = json.loads(f.read())
        self.importer = Importer(database_type="bigquery", credentials={
            "gcloud_credentials": self.config["gcloud_credentials"], 
            "gcloud_project": self.config["gcloud_project"]
        })

    def test_config_file_exists(self):
        self.assertTrue(os.path.exists('config.json'))

    def test_open_config(self):
        open_config()

class TestDatabaseConnection(unittest.TestCase):
    '''Load several example config files to test for proper connection success / fail responses'''
    def test_connection(self):
        # self.importer
        pass

class TestSQLExecution(DataImporterTest):
    '''Test various SQL commands that are expected to succeed or fail'''
    def test_all_tables(self):
        for db in self.config["databases"]:
            for table in self.config["tables"]:
                sql = f'SELECT * FROM {db}.{table} LIMIT 1;'
                self.importer._execute(sql)

class TestDataTranformation(DataImporterTest):
    '''Given data in the form of a database response, test that it transforms into the expected format'''
    # to test: data in valid format? data in readable json after run() completed?
    def test_label_extractor_test(self):
        importer = Importer(database_type="bigquery", label_extractor="label_extractor", credentials={
            "gcloud_credentials": self.config["gcloud_credentials"], 
            "gcloud_project": self.config["gcloud_project"]
        })

    def test_transform(self):
        pass

class TestRun(DataImporterTest):
    def test_run(self):
        db = self.config["databases"][0]
        table = self.config["tables"][0]
        self.importer.run(f'SELECT * FROM {db}.{table} LIMIT 1;')

if __name__=="__main__":
    unittest.main()