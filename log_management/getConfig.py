import json

config = None

with open('/etc/config/config.json') as f:
    config = json.load(f)
