import logging
from processing.utilities import *


class Processor:
    def __init__(self, config: dict, df: pd.DataFrame = None, db: dict = None):
        self.logger = logging.getLogger('CPAS')
        self.config = config
        if df is not None:
            self.logger.info("User precessing a dataframe...")
            self.df = df
            self.db = None
            self.processing_df()
        else:
            self.logger.info("User precessing data through database")
            assert db is not None, "ERROR: Processor launched with no dataframe nor database settings."
            self.db = db
            self.df = None

    def processing_df(self):
        self.df = self.df.drop(columns=self.config['drop'], axis=1)

        self.df = self.df.drop_duplicates()
        self.logger.info('Duplicates dropped.')

        if self.config.get('remove_outliers', {}):
            self.logger.info(f'User turned on outlier detection mechanism with the following settings: '
                             f'{self.config['remove_outliers']}')
            threshold = self.config['remove_outliers']['threshold']
            for col in self.config['remove_outliers']['columns']:
                self.df = remove_outliers(self.df, col, threshold)

        if self.config['remove_unnecessary_labels'].lower() == 'true':
            self.df = self.df[~self.df['libelle'].str.contains('ARRETE', case=False, na=False)]
            self.logger.info('Unnecessary labels dropped.')

        # cast date
        self.df['date_pc'] = pd.to_datetime(self.df['date_pc'], format='%m/%d/%y')

        self.fill_empty_client_codes()


    def fill_empty_client_codes(self):
        self.df['auxiliaire'] = self.df['auxiliaire'].replace("None", pd.NA)

        is_client_row = self.df['auxiliaire'].notna()
        group_ids = is_client_row[::-1].cumsum()[::-1]
        self.df['group'] = group_ids

        num_groups = self.df['group'].nunique()

        self.logger.info(f'Identified {num_groups} groups of transactions in the data.')

        records = []
        nb = 0
        for _, group_df in self.df.groupby('group'):
            # Below condition to limit number of groups when testing -- Need to be changed for full run
            if nb == 1000:
                break
            client_row = group_df[group_df['auxiliaire'].notna()].iloc[0]
            transactions = group_df[group_df['auxiliaire'].isna()]

            client_code = client_row['auxiliaire']
            client_name = client_row['libelle']
            date = client_row['date_pc']

            record = {
                'client_id': client_code,
                'client_name': client_name[0:14].strip(),
                'date': date,
            }

            total = 0.0
            for _, row in transactions.iterrows():
                item = row['libelle']
                amount = row['montant']
                if item and pd.notna(amount):
                    record[item] = amount
                    total += amount

            record['total_amount'] = total
            records.append(record)

            nb += 1
            self.logger.info(f"Processed group {nb} out of {num_groups} groups.")

        result = pd.DataFrame(records)

        col = 'total_amount'
        result = result[[c for c in result.columns if c != col] + [col]]
        result.to_csv('results.csv', index=False)


    def processing_db(self):
        # TODO
        pass
