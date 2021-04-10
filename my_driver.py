from Utils.parallelize_utils import ProcessUtils
import time


database_params = {'server': 'localhost', 'user_name': 'root', 'port': '3306', 'database': 'my_schema', 'table': 'products'}
file_path = '/Users/Jewel/Interview/Postman/Assignments/products_test.csv'
chunksize = 62_500  # Optimal chunk size = Total data rows/cpu cores available

if __name__ == "__main__":
    tic = time.perf_counter()

    process_utils = ProcessUtils(file_path=file_path, chunksize=chunksize, database_params=database_params)
    reader = process_utils.read_data_to_df()

    # Insert data from csv to table
    process_utils.parallelize_insert(insert_function=process_utils.insert_data, data_chunks=reader, timeout=60)

    # Update data based on one or more keys
    update_key = {'sku': 'citizen-some-middle', 'name': 'Roger Huerta'}
    update_values = {'name': 'Test name', 'description': "This is a product description for test"}
    process_utils.update_data(update_key=update_key, update_values=update_values)

    toc = time.perf_counter()

    print(f'Chunk_size: {chunksize}, time_elapsed: {toc - tic}')
