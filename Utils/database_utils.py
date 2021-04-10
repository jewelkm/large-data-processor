import pandas as pd
import sqlalchemy
import hashlib
from sqlalchemy import exc

from common_utils import *

class DatabaseUtils:
    def __init__(self, file_path, chunksize, database_params):
        self.file_path = file_path
        self.df = pd.DataFrame()
        self.chunksize = chunksize
        self.database_params = database_params

    def read_data_to_df(self):
        try:
            self.df = pd.read_csv(self.file_path, chunksize=self.chunksize)
            return self.df
        except Exception as e:
            return str(e)

    @staticmethod
    def get_shape(df):
        return df.shape[0]

    @staticmethod
    def insert_data(df, database, table, db_uri):
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

    @staticmethod
    def clear_table(db_uri):
        engine = sqlalchemy.create_engine(db_uri)
        sql = f"truncate table my_schema.products;"
        engine.execute(sql)

    @staticmethod
    def update_data(update_key, update_values, db_uri, database_params):
        engine = sqlalchemy.create_engine(db_uri)

        final_where_cond = create_cond_string(update_key, flag='where')
        read_sql = f"SELECT * FROM {database_params['database']}.{database_params['table']} WHERE {final_where_cond};"

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
            update_sql = f"UPDATE {database_params['database']}.{database_params['table']} SET {final_set_cond} WHERE {final_where_cond};"

            # Update the database
            engine.execute(update_sql)



