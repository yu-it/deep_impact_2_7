# -*- coding:utf-8 -*-

import deep_impact_2_7.bq.bq as bq
import deep_impact_2_7.util as util
import collections


auth_info = collections.namedtuple("auth_info", [
                        "user_name",
                        "password"
                    ])

def query():
    return bq.selectFromBq(auth_info, sql)[0]  # target_column_ambigous:キャストエラーが出ることあり

sql = """
#standardsql
select * from jrdb_raw_data_schema_info.auth_info
"""
