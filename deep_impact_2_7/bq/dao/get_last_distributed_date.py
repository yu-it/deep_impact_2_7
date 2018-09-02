# -*- coding:utf-8 -*-

import deep_impact_2_7.bq.bq as bq
import deep_impact_2_7.util as util
import collections


def query(table_name):
    try:
        return bq.selectFromBqAsArray(sql.format(table_name = table_name))[0][0]  # target_column_ambigous:キャストエラーが出ることあり
    except Exception as ex:
        return None

sql = """
#standardsql
select max(distributed_date) from jrdb_raw_data.{table_name}
"""
