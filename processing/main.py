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

        if self.config['remove_outliers']:
            self.logger.info(f'User turned on outlier detection mechanism with the following setting: '
                             f'{self.config['remove_outliers']}')
            threshold = self.config['remove_outliers']['threshold']
            for col in self.config['remove_outliers']['columns']:
                self.df = remove_outliers(self.df, col, threshold)

        if self.config['remove_unnecessary_labels'].lower() == 'true':
            self.df = self.df[~self.df['libelle'].str.contains('ARRETE', case=False, na=False)]
            self.logger.info('Unnecessary labels dropped.')

        # cast date
        self.df['date_pc'] = pd.to_datetime(self.df['date_pc'], format='%m/%d/%y')

    def processing_db(self):
        # TODO
        pass
