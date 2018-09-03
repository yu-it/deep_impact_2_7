from google.cloud import bigquery
import deep_impact_2_7.util as util
DataCharisticsQuery = """
#standardSQL
select sum(length) over (partition by null) + 2 record_length, c.* from jrdb_raw_data_schema_info.data_characteristics c where table_name = '{table_name}'order by seq
"""
DataCharisticsColumnSpecifiedQuery = """
#standardSQL
select sum(length) over (partition by null) + 2 record_length, c.* from jrdb_raw_data_schema_info.data_characteristics c where table_name = '{table_name}' and column_pysical_name = '{column_name}' order by seq
"""
DataCharacteristicsQuery_record_length = 0


def selectFromBq(dto_class, query, asdict = False):
    ret = []
    for raw_record in selectFromBqAsArray(query):
        if asdict:
            ret.append(dto_class(*raw_record)._asdict())
        else:
            ret.append(dto_class(*raw_record))
    return ret

def selectFromBqAsArray(query):
    util.debug(query)
    client = bigquery.Client(project='yu-it-base')
    query_job = client.run_sync_query(query,)  # API request - starts the query
    query_job.run()
    return query_job.rows


def get_data_charistics(dataset_name, table_name, column_name = None):

    chars = selectFromBqAsArray(DataCharisticsQuery.format(table_name=table_name)) if column_name is None else \
        selectFromBqAsArray(DataCharisticsColumnSpecifiedQuery.format(table_name=table_name, column_name=column_name))
    return {"characteristic": chars,
            "record_length": chars[0][DataCharacteristicsQuery_record_length]}


