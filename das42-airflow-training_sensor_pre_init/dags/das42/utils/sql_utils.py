import os
import logging

from das42.utils.utils import Utils

class SqlUtils(Utils):
    @staticmethod
    def load_query(job_name):
        dir_name = os.path.dirname(__file__)
        path = os.path.join(
            dir_name,
            "..",
            "sql",
            job_name + ".sql"
        )
        logging.debug("reading sql file: %s", path)
        sql_file = open(path, "r")
        sql_text = sql_file.read()
        sql_file.close()
        return sql_text
