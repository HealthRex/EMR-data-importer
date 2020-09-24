import json
from pathlib import Path

class Importer():
    def __init__(self, config):
        with open(config) as f:
            j = json.loads(f.read())
        self.endpoint = j['endpoint']
        self.query = j['query']
        self.results_fname = j['results']
        
        self._connect()

    def _connect(self):
        print("todo: write connection code")

    def _execute(self):
        print("todo: write execution code")

    def _transform(self):
        print("todo: write transformation code")

    def run(self):
        '''function for executing the given SQL query and returning the transformed data'''
        self._execute()
        self._transform()
        with open(self.results_fname, 'w') as f:
            f.write('{}')
        return Path(self.results_fname)

if __name__=="__main__":
    print("Running importer")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='the location of the config file', required=True)

    args = parser.parse_args()

    imp = Importer(args.config)
    path = imp.run()
    print("Results at", path)
