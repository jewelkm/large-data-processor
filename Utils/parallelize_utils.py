import multiprocessing as mp
from Utils.database_utils import DatabaseUtils


class ProcessUtils(DatabaseUtils):

    def parallelize_insert(self, insert_function, data_chunks, timeout):
        """
        Parallelize the insert function using Multiprocessing module in python.
        :param insert_function: (Function Object) Insert function
        :param data_chunks: (Object) Chunks of pandas dataframe data
        :param timeout: (Integer) Time to wait for multiprocessing to complete
        :return: (None)
        """
        funclist = []
        pool = mp.Pool(mp.cpu_count())

        for df in data_chunks:
            try:
                f = pool.apply_async(insert_function, [df, self.database_params['database'], self.database_params['table'], self.db_uri])
                funclist.append(f)
            except Exception as e:
                raise Exception(e)

        for f in funclist:
            f.get(timeout=timeout)
