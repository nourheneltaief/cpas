import logging
from utilities import *


class Processor:
    def __init__(self, config: dict, df: pd.DataFrame = None, db: dict = None):
        self.logger = logging.getLogger(__name__)
        self.config = config
        if df:
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
        self.df = self.df.drop(columns=self.config['analysis']['drop'], axis=1)

        self.df = self.df.drop_duplicates()
        self.logger.info('Duplicates dropped.')

        if self.config['analysis']['remove_outliers']:
            self.logger.info(f'User turned on outlier detection mechanism with the following setting: '
                             f'{self.config['analysis']['remove_outliers']}')
            threshold = self.config['analysis']['remove_outliers']['threshold']
            for col in self.config['analysis']['remove_outliers']['columns']:
                self.df = remove_outliers(self.df, col, threshold)

        if self.config['analysis']['remove_unnecessary_labels'].lower() == 'true':
            self.df = self.df[~self.df['libelle'].str.contains('ARRETE', case=False, na=False)]
            self.logger.info('Unnecessary labels dropped.')

        # cast date
        self.df['date_pc'] = pd.to_datetime(self.df['date_pc'], format='%m/%d/%Y')

    def processing_db(self):
        # TODO
        pass
