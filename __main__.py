from .emr_importer import Importer
import json
import os

try:
    with open(os.path.join(os.path.dirname(__file__),"config.json"), 'r') as f:
        config = json.loads(f.read())
except:
        print("no config file found")
imp = Importer(**config)
path = imp.run()
print("Results at", path)
