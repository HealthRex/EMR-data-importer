from .emr_importer import Importer
import json
import os


import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--config', help='the location of the config file', required=False)
args = parser.parse_args()

try:
    print("opening", args.config)
    with open(args.config) as f:
        config = json.loads(f.read())
except:
    print("didn't work, trying for default")
    try:
        with open(os.path.join(os.path.dirname(__file__),"config.json"), 'r') as f:
            config = json.loads(f.read())
    except Exception as e:
            raise Exception("No config file found in package directory or specified on command line") from e
else:
    imp = Importer(**config)
    path = imp.run()
    print("Results at", path)
