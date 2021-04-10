from Utils.database_utils import DatabaseUtils
from Utils.parallelize_utils import ParallelizeUtils
import time


database_params = {'server': 'localhost', 'user_name': 'root', 'port': '3306', 'database': 'my_schema', 'table': 'products'}
file_path = '/Users/Jewel/Interview/Postman/Assignments/products.csv'
chunksize = 62_500

db_uri = f"mysql://{database_params['user_name']}@{database_params['server']}:{database_params['port']}/{database_params['database']}?charset=utf8"

if __name__ == "__main__":
    tic = time.perf_counter()

    data_base_utils = DatabaseUtils(file_path=file_path, chunksize=chunksize, database_params=database_params)
    parallel_utils = ParallelizeUtils()
    reader = data_base_utils.read_data_to_df()

    parallel_utils.parallelize_insert(insert_function=data_base_utils.insert_data, data_chunks=reader,
                                      database_params=database_params, timeout=60)

    # update_key = {'sku': 'citizen-some-middle'}
    # update_values = {'name': 'Ambatti', 'description': "This is ambatti description"}
    # data_base_utils.update_data(update_key=update_key, update_values=update_values, db_uri=db_uri, database_params=database_params)

    toc = time.perf_counter()

    print(f'Chunk_size: {chunksize}, time_elapsed: {toc - tic}')
