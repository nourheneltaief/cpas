import argparse
import os
import yaml
from dotenv import load_dotenv
from staging.main import Dataloader
from processing.main import Processor


def load_config(config_path: str):
    with open(os.path.join(config_path), 'r') as file:
        data = yaml.safe_load(file)
    return data

def load_env_vars(config_dict: dict):
    load_dotenv()
    config_dict['staging']['DB_INFO'] = {
        'DB_HOST': os.getenv("DB_HOST"),
        'DB_PORT': os.getenv("DB_PORT"),
        'DB_NAME': os.getenv("DB_NAME"),
        'DB_USER': os.getenv("DB_USER"),
        'DB_PASSWORD': os.getenv(""),
    }

def run():
    parser = argparse.ArgumentParser(description="Main module to launch CPAS")
    parser.add_argument("--work_dir", type=str, required=True, help="Working directory which contains input"
                                                                    " data")
    parser.add_argument("--config", type=str, required=True, help="Configuration path")
    args = parser.parse_args()
    config_dict = load_config(args.config)
    config_dict['staging']['work_dir'] = args.work_dir
    load_env_vars(config_dict)

    dl = Dataloader(config=config_dict['staging'])

    processor = Processor(config=config_dict['processing'], df=dl.df)


if __name__ == "__main__":
    run()