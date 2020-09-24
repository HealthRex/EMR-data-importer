import emr_importer
import unittest

class TestDatabaseConnection(unittest.TestCase):
    '''Load several example config files to test for proper connection success / fail responses'''
    def setUp(self):
        pass

    def test_connection(self):
        pass

class TestSQLExecution(unittest.TestCase):
    '''Test various SQL commands that are expected to succeed or fail'''
    def setUp(self):
        pass

    def test_sql(self):
        pass

class TestDataTranformation(unittest.TestCase):
    '''Given data in the form of a database response, test that it transforms into the expected format'''
    def setUp(self):
        pass

    # to test: data in valid format? data in readable json after run() completed?
    def test_transform(self):
        pass

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