import pandas as pd
import sqlalchemy
from sqlalchemy import exc

from Utils.common_utils import *


class DatabaseUtils:
    def __init__(self, file_path, chunksize, database_params):
        self.file_path = file_path
        self.chunksize = chunksize
        self.database_params = database_params
        self.db_uri = f"mysql://{database_params['user_name']}@{database_params['server']}:{database_params['port']}/" \
                      f"{database_params['database']}?charset=utf8"
        self.df = pd.DataFrame()

    @staticmethod
    def get_rows(df):
        """
        Read the DataFrame and return the number of rows.
        :param df:(DataFrame)
        :return:(Integer) Rows in DataFrame
        """
        return df.shape[0]

    @staticmethod
    def insert_data(df, database, table, db_uri):
        """
        Inserts the DataFrame to the database and table provided.
        Creates an insert query for each row in dataframe and send to db.
        :param df: (DataFrame) DataFrame to be inserted.
        :param database: (String) Database name
        :param table: (String) Table name
        :param db_uri: (String) Connection uri
        :return: (None)
        """
        try:
            engine = sqlalchemy.create_engine(db_uri)
            df = create_hash_id(df)

            def create_insert_sql(x):
                cols = "`" + "`,`".join(list(df.columns)) + "`"
                values = "\'" + "\',\'".join(list(x)) + "\'"
                sql = f"INSERT INTO `{database}`.`{table}` ({cols}) VALUES ({values});"
                try:
                    engine.execute(sql)
                except exc.IntegrityError:
                    pass

            df.apply(lambda x: create_insert_sql(x), axis=1)
            # df.to_sql(name=table, con=engine, if_exists='append', index=False)
        except Exception as e:
            raise Exception(str(e))

    def read_data_to_df(self):
        """
        Read the csv to DataFrame and return chunks of data.
        Uses the file path and chunksize from Database utils object
        :return: (DataFrame)
        """
        try:
            self.df = pd.read_csv(self.file_path, chunksize=self.chunksize)
            return self.df
        except Exception as e:
            return str(e)

    def clear_table(self, database, table):
        """
        Clear the data in the table
        :return: (None)
        """
        engine = sqlalchemy.create_engine(self.db_uri)
        sql = f"truncate table {database}.{table};"
        engine.execute(sql)

    def update_data(self, update_key, update_values):
        """
        Update the data in table using the provided key and values
        :param update_key: (Dictionary) A dictionary of column names and values to search the db
        :param update_values: (Dictionary) A dictionary of columns and values to be updated for the searched record
        :return: (None)
        """
        engine = sqlalchemy.create_engine(self.db_uri)

        final_where_cond = create_cond_string(update_key, flag='where')
        read_sql = f"SELECT * FROM {self.database_params['database']}.{self.database_params['table']} WHERE {final_where_cond};"

        update_df = pd.read_sql_query(sql=read_sql, con=engine)

        if update_df.shape[0] == 0:
            raise Exception("No data in the database for given key(s)")
        else:
            update_df.drop(columns=['id'], inplace=True)

            for key in update_values:
                update_df[key] = update_values[key]

            # New dataframe for the updated data
            update_df = create_hash_id(update_df)
            update_values['id'] = update_df['id'].iloc[0]

            # Create update query with the update data
            final_set_cond = create_cond_string(update_values, flag='set')
            update_sql = f"UPDATE {self.database_params['database']}.{self.database_params['table']} SET {final_set_cond} WHERE {final_where_cond};"

            # Update the database
            engine.execute(update_sql)
