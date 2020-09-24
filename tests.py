import emr_importer
import unittest

class DataImporterTest(unittest.TestCase):
    def setUp(self):
        with open("test-config.json") as f:
            self.config = json.loads(f.read())
        self.importer = Importer(
            gcloud_credentials = self.config["gcloud_credentials"], 
            gcloud_project = self.config["gcloud_project"]
        )

class TestDatabaseConnection(unittest.TestCase):
    '''Load several example config files to test for proper connection success / fail responses'''
    def test_connection(self):
        self.importer._connect()

class TestSQLExecution(unittest.TestCase):
    '''Test various SQL commands that are expected to succeed or fail'''
    def test_all_tables(self):
        for db in config["databases"]:
            for table in config["tables"]:
                sql = f'SELECT * FROM {db}.{table} LIMIT 1;'
                self.importer._execute(sql)
    
class TestDataTranformation(DataImporterTest):
    '''Given data in the form of a database response, test that it transforms into the expected format'''
    # to test: data in valid format? data in readable json after run() completed?
    def test_transform(self):
        bq_results = [

        ]

def suite():
    """Returns the suite of tests to run for this test class / module.
    Use unittest.makeSuite methods which simply extracts all of the
    methods for the given class whose name starts with "test"
    """
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestDatabaseConnection))
    suite.addTest(unittest.makeSuite(TestSQLExecution))
    suite.addTest(unittest.makeSuite(TestDataTranformation))
    
    return suite
    
if __name__=="__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())
