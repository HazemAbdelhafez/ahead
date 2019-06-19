import sqlite3
import pandas as pd
from strings import *
import copy
from analyzer import Analyzer


class GenericQueries:
    DEVICE_TO_DEVICE_SYMBOL_CPY = "select * from CUPTI_ACTIVITY_KIND_MEMCPY memcpy where memcpy.copyKind ==8 and memcpy.flags==1;"
    DEVICE_TO_DEVICE_CPY = "select * from CUPTI_ACTIVITY_KIND_MEMCPY memcpy where memcpy.copyKind ==8 and memcpy.flags==0;"


class SingleProcessQueries:
    DATA_MOTION_COLLECTIVE_TIME = "select memcpy.copyKind, memcpy.srcKind, memcpy.dstKind, runtime.end - runtime.start " \
                                  "AS '%s', memcpy.end - memcpy.start AS '%s', memcpy.bytes, memcpy.flags " \
                                  "FROM CUPTI_ACTIVITY_KIND_MEMCPY memcpy JOIN CUPTI_ACTIVITY_KIND_RUNTIME runtime " \
                                  "where memcpy.correlationId = runtime.correlationId;" % (CUDA_DM_TIME_STR,
                                                                                           GPU_DM_TIME_STR)

    KERNELS_COLLECTIVE_TIME = "select sum(runtime.end - runtime.start) as '%s', sum(kernel.end - kernel.start) as '%s' from CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL kernel JOIN CUPTI_ACTIVITY_KIND_RUNTIME runtime where kernel.correlationId = runtime.correlationId; " % (
    CUDA_KERNEL_TIME_STR, GPU_KERNEL_TIME_STR)

    APP_TOTAL_TIME = "select max(m1) - min(m2) as '%s' FROM (SELECT max(driver.end) as m1, min(driver.start) as m2 from CUPTI_ACTIVITY_KIND_DRIVER driver UNION SELECT max(runtime.end) as m1, min(runtime.start) as m2 from CUPTI_ACTIVITY_KIND_RUNTIME runtime);" % APP_TIME_STR

    PROCESS_START_END_TIMES = "select min(m2) as '%s', max(m1) as '%s' FROM (SELECT max(driver.end) as m1, min(driver.start) as m2 from CUPTI_ACTIVITY_KIND_DRIVER driver UNION SELECT max(runtime.end) as m1, min(runtime.start) as m2 from CUPTI_ACTIVITY_KIND_RUNTIME runtime);" \
                              % (PROCESS_START, PROCESS_END)

    DRIVER_TABLE_SIZE = "SELECT COUNT(*) AS '%s' from CUPTI_ACTIVITY_KIND_DRIVER driver;" % DRIVER_TABLE_SIZE_STR

    RUNTIME_TABLE_SIZE = "SELECT COUNT(*) AS '%s' from CUPTI_ACTIVITY_KIND_RUNTIME runtime;" % RUNTIME_TABLE_SIZE_STR

    DM_STATS_TABLE = "select COUNT(*) AS '%s' FROM CUPTI_ACTIVITY_KIND_MEMCPY memcpy JOIN CUPTI_ACTIVITY_KIND_RUNTIME runtime where memcpy.correlationId = runtime.correlationId;" % DM_STATS_TABLE_SIZE_STR

    DEVICE_TIME = "SELECT sum(driver.end-driver.start) as '%s' FROM CUPTI_ACTIVITY_KIND_DRIVER driver UNION  SELECT sum(runtime.end-runtime.start) as '%s' FROM CUPTI_ACTIVITY_KIND_RUNTIME runtime;" % (
    DEVICE_TIME_STR, DEVICE_TIME_STR)

    APP_QUERIES = dict()
    DATA_MOTION_QUERIES = dict()

    def __init__(self):
        self.set_queries()

    def set_queries(self):
        self.DATA_MOTION_QUERIES[COLLECTIVE_TIME_STR] = self.DATA_MOTION_COLLECTIVE_TIME
        self.APP_QUERIES[COLLECTIVE_TIME_STR] = self.KERNELS_COLLECTIVE_TIME
        self.APP_QUERIES[APP_TIME_STR] = self.APP_TOTAL_TIME
        self.APP_QUERIES[DEVICE_TIME_STR] = self.DEVICE_TIME
        self.APP_QUERIES[MODE_PER_PROCESS_APP_TIME_STR] = self.PROCESS_START_END_TIMES


class MultiProcessQueries(SingleProcessQueries):
    DATA_MOTION_COLLECTIVE_TIME = "select memcpy.copyKind, memcpy.srcKind, memcpy.dstKind, runtime.end - runtime.start AS '%s', memcpy.end - memcpy.start AS '%s', memcpy.bytes, memcpy.flags FROM CUPTI_ACTIVITY_KIND_MEMCPY memcpy JOIN CUPTI_ACTIVITY_KIND_DRIVER runtime where memcpy.correlationId = runtime.correlationId;" % (
    CUDA_DM_TIME_STR, GPU_DM_TIME_STR)

    KERNELS_COLLECTIVE_TIME = "select sum(runtime.end - runtime.start) as '%s', sum(kernel.end - kernel.start) as '%s' from CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL kernel JOIN CUPTI_ACTIVITY_KIND_RUNTIME runtime where kernel.correlationId = runtime.correlationId; " % (
    CUDA_KERNEL_TIME_STR, GPU_KERNEL_TIME_STR)

    APP_TOTAL_TIME = "SELECT max(driver.end) - min(driver.start) as '%s' from CUPTI_ACTIVITY_KIND_DRIVER driver;" \
                     % APP_TIME_STR

    PROCESS_START_END_TIMES = "SELECT min(driver.start) as '%s', max(driver.end) as '%s' from CUPTI_ACTIVITY_KIND_DRIVER driver;" \
                              % (PROCESS_START, PROCESS_END)

    def __init__(self):
        SingleProcessQueries.__init__(self)
        SingleProcessQueries.set_queries(self)

    def set_queries(self):
        self.DATA_MOTION_QUERIES[COLLECTIVE_TIME_STR] = self.DATA_MOTION_COLLECTIVE_TIME
        self.APP_QUERIES[COLLECTIVE_TIME_STR] = self.KERNELS_COLLECTIVE_TIME
        self.APP_QUERIES[APP_TIME_STR] = self.APP_TOTAL_TIME
        self.APP_QUERIES[MODE_PER_PROCESS_APP_TIME_STR] = self.PROCESS_START_END_TIMES


class CombinedProcessQueries(SingleProcessQueries):
    KERNELS_COLLECTIVE_TIME = "SELECT * from (select sum(runtime.end-runtime.start) as '%s', sum(kernel.end - kernel.start) as '%s' from CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL kernel JOIN CUPTI_ACTIVITY_KIND_DRIVER runtime where kernel.correlationId = runtime.correlationId UNION ALL select sum(runtime.end - runtime.start) as '%s', sum(kernel.end - kernel.start) as '%s' from CUPTI_ACTIVITY_KIND_CONCURRENT_KERNEL kernel  JOIN CUPTI_ACTIVITY_KIND_RUNTIME runtime where kernel.correlationId = runtime.correlationId); " \
                              % (CUDA_KERNEL_TIME_STR, GPU_KERNEL_TIME_STR, CUDA_KERNEL_TIME_STR, GPU_KERNEL_TIME_STR)

    def __init__(self):
        SingleProcessQueries.__init__(self)
        SingleProcessQueries.set_queries(self)

    def set_queries(self):
        self.DATA_MOTION_QUERIES[COLLECTIVE_TIME_STR] = self.DATA_MOTION_COLLECTIVE_TIME
        self.APP_QUERIES[COLLECTIVE_TIME_STR] = self.KERNELS_COLLECTIVE_TIME


class Filter:
    query_string = ""
    db_paths = ""
    connection = sqlite3.Connection
    data_frame = None

    def __init__(self, _db_paths, _query):
        self.db_paths = _db_paths
        self.query_string = _query

    def connect(self):
        self.connection = sqlite3.connect(self.db_paths)

    def get_data_as_data_frame(self):
        if self.data_frame is not None:
            return self.data_frame.dropna()  # Filter out NaN values
        else:
            return Analyzer.read_trace_files(self.db_paths, self.query_string).dropna()  # Filter out NaN values

    def set_query_statement(self, mode=COLLECTIVE_TIME_STR):
        self.query_string = ""

    def close(self):
        self.connection.close()

    @staticmethod
    def get_data_frame_from_file(_file, _query):
        connection = sqlite3.connect(_file)
        df = pd.read_sql_query(_query, connection)
        connection.close()
        return copy.deepcopy(df)

    @staticmethod
    def get_data_frame_from_files(_paths, _query):
        if type(_paths) is list:
            paths_iter = iter(_paths)
            _path = next(paths_iter)
            _df_merged = Analyzer.read_trace_file(_path, _query)
            for _path in paths_iter:
                _df = Analyzer.read_trace_file(_path, _query)
                _df_merged = pd.concat([_df_merged, _df], ignore_index=True)
        else:
            _df_merged = Analyzer.read_trace_file(_paths, _query)
        _data_frame = _df_merged
        return copy.deepcopy(_data_frame)

    def select_app_time_stats_table(self, _paths):

        # Check the driver and runtime tables size
        _query = SingleProcessQueries.DRIVER_TABLE_SIZE
        _df = self.get_data_frame_from_files(_paths, _query)
        driver_table_size = int(_df.to_dict('record')[0][DRIVER_TABLE_SIZE_STR])

        _query = SingleProcessQueries.RUNTIME_TABLE_SIZE
        _df_1 = self.get_data_frame_from_files(_paths, _query)
        runtime_table_size = int(_df_1.to_dict('record')[0][RUNTIME_TABLE_SIZE_STR])

        if driver_table_size == 0 and runtime_table_size != 0:
            return SingleProcessQueries()
        elif runtime_table_size == 0 and driver_table_size != 0:
            return MultiProcessQueries()
        elif driver_table_size != 0 and runtime_table_size != 0:
            return CombinedProcessQueries()
        else:
            print("Driver table size: %d, Runtime table size: %d" % (driver_table_size, runtime_table_size))
            print("Invalid SQLITE tables in queries.py")
            exit(0)

    def select_dm_stats_table(self, _paths):

        # Check the driver and runtime tables size
        _query = SingleProcessQueries.DM_STATS_TABLE
        _df = self.get_data_frame_from_files(_paths, _query)
        dm_table_size = int(_df.to_dict('record')[0][DM_STATS_TABLE_SIZE_STR])

        if dm_table_size == 0:
            return MultiProcessQueries()
        else:
            return SingleProcessQueries()


class DataMotionFilter(Filter):

    def __init__(self, paths, mode=COLLECTIVE_TIME_STR):
        self.queries = self.select_dm_stats_table(paths)
        Filter.__init__(self, paths, self.queries.DATA_MOTION_QUERIES[mode])

    def set_query_statement(self, mode=COLLECTIVE_TIME_STR):
        if mode != self.query_string:
            self.data_frame = None  # Invalidate the current data frame if we are querying a different thing
        self.query_string = self.queries.DATA_MOTION_QUERIES[mode]


class ComputeFilter(Filter):
    def __init__(self, paths, mode=COLLECTIVE_TIME_STR):
        self.queries = self.select_app_time_stats_table(paths)
        Filter.__init__(self, paths, self.queries.APP_QUERIES[mode])

    def set_query_statement(self, mode=COLLECTIVE_TIME_STR):
        if mode != self.query_string:
            self.data_frame = None  # Invalidate the current data frame if we are querying a different information
        self.query_string = self.queries.APP_QUERIES[mode]


if __name__ == "__main__":
    pass
