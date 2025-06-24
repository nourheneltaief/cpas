import psycopg2
import logging
import os
import re
from collections import defaultdict
import pandas as pd
from sqlalchemy import create_engine

from utilities import *

class Dataloader:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.config = config

        self.set_logger_config()
        # test connection to db
        self.test_db_connection()

        self.pattern = re.compile(r"(\d{4})-(\d+)\.txt?$")
        self.data_dict = self.get_data_dict()

        rows = self.get_rows_list()

        table_name = self.config['data']['output']['name']
        drop_table = drop_table_if_exists_query(table_name)
        self.execute_query(drop_table)

        create_table = create_table_query(table_name)
        self.execute_query(create_table)

        if self.config['use_pandas_interim'].lower() == 'true':
            self.logger.info('Using pandas as interim for easy transformations...')
            self.df = self.rows_to_df(rows)
            self.logger.info('Dataframe created.')
            # TODO remove_outliers in pandas then send df to database

            self.df = self.df.drop_duplicates()
            self.logger.info('Duplicates dropped.')

            if self.config['remove_unnecessary_labels'].lower() == 'true':
                self.df = self.df[~self.df['libelle'].str.contains('ARRETE', case=False, na=False)]
                self.logger.info('Unnecessary labels dropped.')

            engine = self.create_engine()

            self.logger.info('Sending dataframe to db...')
            self.df.to_sql(table_name, engine, if_exists='replace', index=False)
            self.logger.info('Dataframe sent to db.')

        else:
            insert_data = insert_all_rows(table_name)
            self.execute_query(insert_data, many=True, data=rows)

            if self.config['remove_unnecessary_labels'].lower() == 'true':
                remove_query = remove_unnecessary_labels(table_name)
                self.execute_query(remove_query)


    def set_logger_config(self):
        log_level = self.config['log_level']
        self.logger.setLevel(log_level)
        handler = logging.StreamHandler()
        handler.setLevel(log_level)

        # Create and set a formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # Add the handler to the logger (avoid duplicates!)
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
        dir_ = os.path.join(self.config['work_dir'], self.config['data']['input_dir'])
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
                file_path = os.path.join(self.config['work_dir'], self.config['data']['input_dir'], filename)
                with open(file_path, 'r') as file:
                    lines = file.readlines()
                rows.extend(get_lines_info(lines))

        return rows

    def rows_to_df(self, rows):
        return pd.DataFrame(rows, columns=self.config['data']['columns'])

    def create_engine(self):
        username = self.config['DB_USER']
        pwd = self.config['DB_PASSWORD']
        host = self.config['DB_HOST']
        port = self.config['DB_PORT']
        dbname = self.config['DB_NAME']
        return create_engine(f'postgresql+psycopg2://{username}:{pwd}@{host}:{port}/{dbname}')