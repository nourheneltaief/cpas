import psycopg2
import logging
import os
import re
from collections import defaultdict
from sqlalchemy import create_engine
import pandas as pd

from staging import utilities


class Dataloader:
    def __init__(self, config):
        self.logger = logging.getLogger('CPAS')
        self.config = config

        self.set_logger_config()

        self.pattern = re.compile(r"(\d{4})-(\d+)\.txt?$")
        self.data_dict = self.get_data_dict()

        rows = self.get_rows_list()

        if self.config['use_pandas'].lower() == 'true':
            self.logger.info('Using dataframes...')
            self.df = self.rows_to_df(rows)
            self.logger.info('Dataframe created.')
        else:
            self.logger.info('Using database tables...')

            # test connection to db
            self.test_db_connection()

            table_name = self.config['output']['name']

            drop_table = utilities.drop_table_if_exists_query(table_name)
            self.execute_query(drop_table)

            create_table = utilities.create_table_query(table_name)
            self.execute_query(create_table)

            insert_data = utilities.insert_all_rows(table_name)
            self.execute_query(insert_data, many=True, data=rows)

            remove_query = utilities.remove_unnecessary_labels(table_name)
            self.execute_query(remove_query)


    def set_logger_config(self):
        log_level = self.config['log_level']
        self.logger.setLevel(log_level)
        handler = logging.StreamHandler()
        handler.setLevel(log_level)

        # Create and set a formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # Add the handler to the logger
        if not self.logger.hasHandlers():
            self.logger.addHandler(handler)

    def test_db_connection(self):
        self.logger.info("Testing database connection...")
        try:
            conn = psycopg2.connect(
                dbname=self.config['DB_NAME'],
                user=self.config['DB_USER'],
                password=self.config['DB_PASSWORD'],
                host=self.config['DB_HOST'],
                port=self.config['DB_PORT']
            )
            conn.close()
            self.logger.info("Connection successful to the database.")
        except Exception as e:
            self.logger.error(f'Error when testing connection to the database. Please check env file.\n [exception={e}]')

    def execute_query(self, query:str, many:bool = False, data:list = None):
        try:
            conn = psycopg2.connect(
                dbname=self.config['DB_NAME'],
                user=self.config['DB_USER'],
                password=self.config['DB_PASSWORD'],
                host=self.config['DB_HOST'],
                port=self.config['DB_PORT']
            )
            cur = conn.cursor()

            if many and data:
                cur.executemany(query, data)
            else:
                cur.execute(query)

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            self.logger.error(f'Error when connecting to the database to execute [query={query}]. Please check .env file.'
                              f'\n [exception={e}]')


    def get_data_dict(self):
        files_by_code = defaultdict(list)
        dir_ = os.path.join(self.config['work_dir'], self.config['input_dir'])
        for filename_ in os.listdir(dir_):
            match = self.pattern.match(filename_)
            if match:
                year, code_ = match.groups()
                files_by_code[code_].append(filename_)
        for code in files_by_code.keys():
            years = len(files_by_code[code])
            self.logger.info(f'Found {years} years for agency code {code}')
        return files_by_code


    def get_rows_list(self):
        rows = []
        for code in self.data_dict.keys():
            self.logger.info(f"Extracting data [code={code}]...")
            for filename in self.data_dict[code]:
                self.logger.info(f"     Extracting data for year={filename[0: 4]}")
                file_path = os.path.join(self.config['work_dir'], self.config['input_dir'], filename)
                with open(file_path, 'r') as file:
                    lines = file.readlines()
                rows.extend(utilities.get_lines_info(lines))

        return rows

    def rows_to_df(self, rows):
        return pd.DataFrame(rows, columns=self.config['columns'])

    def create_engine(self):
        conf = self.config['DB_INFO']
        username = conf['DB_USER']
        pwd = conf['DB_PASSWORD']
        host = conf['DB_HOST']
        port = conf['DB_PORT']
        dbname = conf['DB_NAME']
        return create_engine(f'postgresql+psycopg2://{username}:{pwd}@{host}:{port}/{dbname}')