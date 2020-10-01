from .emr_importer import Importer
import json
import os


import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--config', help='the location of the config file', required=False)

try:
    with open(args.config) as f:
        config = json.loads(f.read())
except:
    try:
        with open(os.path.join(os.path.dirname(__file__),"config.json"), 'r') as f:
            config = json.loads(f.read())
    except:
            print("no config file found")
finally:
    imp = Importer(**config)
    path = imp.run()
    print("Results at", path)
