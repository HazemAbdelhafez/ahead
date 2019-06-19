import copy
import sqlite3
import logging
import pandas as pd

# Logger initialization
logging_level = logging.DEBUG
if logging_level == logging.DEBUG:
    logging_format = '%(asctime)s [%(levelname)s] File %(filename)s Line No. %(lineno)d  %(message)s'
else:
    logging_format = '%(asctime)s [%(levelname)s] %(message)s'

logging.basicConfig(format=logging_format,
                    datefmt="%Y-%m-%d-%H-%M-%S")
logger = logging.getLogger('analyzer')
logger.setLevel(logging_level)


class Analyzer:
    def __init__(self):
        pass

    @staticmethod
    def read_trace_file(_file, _query):
        try:
            connection = sqlite3.connect(_file)
            df = pd.read_sql_query(_query, connection)
            connection.close()
            return df
        except sqlite3.OperationalError as oe:
            logging.error(oe)
            logging.warning("Unable to open SQLITE file %s. Exiting until error is fixed." % _file)
            exit(-1)

    @staticmethod
    def read_trace_files(_paths, _query):
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

