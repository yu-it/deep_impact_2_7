import deep_impact_2_7.bq.bq as bq
import collections
import json
from google.cloud.bigquery.schema import SchemaField


DataCharisticsQuery = """
#standardSQL
select sum(length) over (partition by null) + 2 record_length, c.* from jrdb_raw_data_schema_info.data_characteristics c where table_name = '{table_name}'order by seq
"""
DataCharisticsColumnSpecifiedQuery = """
#standardSQL
select sum(length) over (partition by null) + 2 record_length, c.* from jrdb_raw_data_schema_info.data_characteristics c where table_name = '{table_name}' and column_pysical_name = '{column_name}' order by seq
"""

data_charistics_dto = collections.namedtuple("data_characteristics", [
    "record_length",
    "table_name",
    "column_pysical_name",
    "column_logical_name",
    "seq",
    "length",
    "start_position",
    "note",
    "type",
    "allow_zero",
    "illegal_value_definition",
    "illegal_value_condition",
    "original_translation"
])


def get_data_charistics(dataset_name, table_name, column_name = None):

    chars = bq.selectFromBq(data_charistics_dto, DataCharisticsQuery.format(table_name=table_name)) if column_name is None else \
        bq.selectFromBq(data_charistics_dto ,DataCharisticsColumnSpecifiedQuery.format(table_name=table_name, column_name=column_name))
    return chars


def as_table_schema_object(schema):
    schema_obj =  [SchemaField(entry["name"],entry["type"]) for entry in json.loads(schema)]
    return schema_obj
def as_simple_table_schema_expression(schema):
    schema_obj =  ",".join(["{name}:{type}".format(name = entry["name"],type = entry["type"]) for entry in json.loads(schema)])
    return schema_obj
