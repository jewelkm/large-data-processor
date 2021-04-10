import multiprocessing as mp


class ParallelizeUtils:

    @staticmethod
    def parallelize_insert(insert_function, data_chunks, database_params, timeout):

        db_uri = f"mysql://{database_params['user_name']}@{database_params['server']}:{database_params['port']}/{database_params['database']}?charset=utf8"
        pool = mp.Pool(mp.cpu_count())

        funclist = []

        for df in data_chunks:
            try:
                f = pool.apply_async(insert_function, [df, database_params['database'], database_params['table'], db_uri])
                funclist.append(f)
            except Exception as e:
                raise Exception(e)

        for f in funclist:
            f.get(timeout=timeout)



