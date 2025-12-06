import yaml
import os

config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
with open(config_path, 'r') as f:
    CONFIG = yaml.safe_load(f)

ENV = {
    'project_id': CONFIG['gcp']['project_id'],
    'region': CONFIG['gcp']['region'],
    'bucket': CONFIG['storage']['bucket_name'],
    'dataset': CONFIG['bigquery']['dataset'],
    'credentials': CONFIG['gcp'].get('credentials_path', '')
}

