import argparse
import os
import yaml
from main import Dataloader
from dotenv import load_dotenv

def load_env_vars(config_dict: dict):
    load_dotenv()
    config_dict['DB_HOST'] = os.getenv("DB_HOST")
    config_dict['DB_PORT'] = os.getenv("DB_PORT")
    config_dict['DB_NAME'] = os.getenv("DB_NAME")
    config_dict['DB_USER'] = os.getenv("DB_USER")
    config_dict['DB_PASSWORD'] = os.getenv("DB_PASSWORD")
    return config_dict

def load_config(config_dict: dict):
    with open(os.path.join('configuration', 'config.yml'), 'r') as file:
        data = yaml.safe_load(file)
    config_dict.update(data)


def run():
    parser = argparse.ArgumentParser(description="Staging module to load SOCOTU raw txt input")
    parser.add_argument("--work_dir", type=str, required=True, help="Working directory which contains input"
                                                                    " data")

    parser.add_argument("--log_level", type=str, required=False, help="Log level to control prints.")
    args = parser.parse_args()
    config_dict = {'work_dir': args.work_dir}
    log_level = 10 # DEBUG LEVEL
    if args.log_level:
        log_level = int(args.log_level)
    config_dict["log_level"] = log_level
    load_config(config_dict)
    return load_env_vars(config_dict)

if __name__ == "__main__":
    config = run()
    Dataloader(config)