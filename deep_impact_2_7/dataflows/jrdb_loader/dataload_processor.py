# -*- coding: utf-8 -*-
#
"""
csvファイルとロード先テーブル名をもらってデータをロードする

1.テーブル名をキーに
　jrdb_raw_data_schema_info.data_charisticsをロードする。
2.data_charisticsの桁数定義と型定義に従いデータを変換する
3.ロードする
4,独自変換があれば行う

"""

# from __future__ import absolute_import
import argparse
import logging
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from deep_impact_2_7.bq.schemaUtil import get_data_charistics
import deep_impact_2_7.bq.categories as categories

import deep_impact_2_7.bq.dao.get_auth_info as get_auth_info
import deep_impact_2_7.bq.dao.get_last_distributed_date as dao_get_last_distributed_date

import deep_impact_2_7.dataflows.jrdb_loader.jrdb_file_scraper as scraper
import datetime
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import WriteToBigQuery

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from deep_impact_2_7.bq import bq
import deep_impact_2_7.util as util
import collections

DataCharacteristicsQuery_column_pysical_name = 2
DataCharacteristicsQuery_length = 5
DataCharacteristicsQuery_start_position = 6
DataCharacteristicsQuery_type = 8
DataCharacteristicsQuery_allow_zero = 9
DataCharacteristicsQuery_illegal_value_definition = 10
DataCharacteristicsQuery_illegal_value_condition = 11
DataCharacteristicsQuery_original_translation = 12


def file_to_recordset(data_entry,record_length):
    records = [(data_entry.file_name,data_entry.byte_seq[idx * record_length : (1 + idx) * record_length]) for idx in range(len(data_entry.byte_seq) / record_length)]
    return records


def special_translation(table_name,column_name, value):
    pass

def convert_to_datetime(column_value,type_detail):

    if len(column_value) == 4 and type_detail == categories.data_characteristics_type.datetime:
        return str(datetime.datetime(int(column_value),1,1))

    if len(column_value) == 4 and type_detail == categories.data_characteristics_type.time:
        return str(datetime.datetime(1990, 1, 1,int(column_value[0:2]),int(column_value[2:4])))

    if len(column_value) == 8 and type_detail == categories.data_characteristics_type.datetime:
        return str(datetime.datetime(int(column_value[0:4]),int(column_value[4:6]),int(column_value[6:8])))

def map_to_record(recordentry, charistics):
    derive_from = recordentry[0]
    recordstring = recordentry[1]
    record = {}
    for idx,entry in enumerate(charistics[0:-1]):

        util.debug("No{idx} position:{x}-{y}".format(
            idx = idx,
            x = entry.start_position - 1,
            y = (entry.start_position + entry.length) - 1))
        try:
            column_value = str.decode(
                recordstring[
                    entry.start_position - 1
                    :
                    (entry.start_position + entry.length) - 1
                ], "ms932"
            ).strip()
            if entry.type in (categories.data_characteristics_type.datetime, categories.data_characteristics_type.time):
                column_value = convert_to_datetime(column_value,entry.type)
            if entry.original_translation <> u"":
                column_value = eval(entry.original_translation,{"x":column_value})
            if entry.allow_zero == u"0" and column_value == "0":
                column_value = ""
            if entry.illegal_value_condition <> u"" and eval(
                    entry.illegal_value_condition,{"x":column_value}):
                column_value = ""

        except Exception as ex:
            column_value = "-"
            util.alert("detect {exname} at No{idx} position:{x}-{y},sequence:{seq}".format(exname=str(ex), idx = idx, x =entry[6] - 1, y =(entry[6] + entry[5]) - 1, seq=recordstring[entry[6] - 1:(entry[6] + entry[5]) - 1]))
        record.update({entry.column_pysical_name:column_value})

    record.update({"distributed_date":derive_from})
    return record

def get_last_distributed_date(table_name):
    from_date = dao_get_last_distributed_date.query(table_name)
    if from_date is None:
        from_date = "19900101"
    else:
        from_date = (from_date + datetime.timedelta(days=1)).strftime('%Y%m%d')
    return from_date


#def run(dataset_name, table_name, from_date, to_date,truncate = False):
def run(pipeline_parameter):
    dataset = pipeline_parameter.dataset_name.get()
    table_name = pipeline_parameter.table_name.get()
    location = pipeline_parameter.location.get()
    from_date = pipeline_parameter.from_date.get()
    to_date = pipeline_parameter.to_date.get()

    util.info("{dataset}.{table}({from_date} to {to_date}) load start".format(
        dataset = dataset_name.get(),
        table = table_name,
        from_date = from_date,
        to_date = to_date)
    )

    #characteristics = bq.get_data_charistics(dataset_name, table_name)
    characteristics = get_data_charistics(dataset_name, table_name)
    auth_data = get_auth_info.query()
    if from_date is None or from_date == "":
        from_date = get_last_distributed_date(table_name)
        truncate = True
    else:
        truncate = False
    if to_date is None or to_date == "":
        to_date = "99999999"
    pipeline_parameter.view_as(SetupOptions).save_main_session = True


    requests = scraper.scraping_request(table_name, from_date, to_date)
    jrdb_auth_info = scraper.jrdb_auth_info(auth_data.user_name, auth_data.password)
    with beam.Pipeline(options=pipeline_parameter) as p:
        lines = (p
                 | beam.Create(
                    requests))
        # Count the occurrences of each word.
        records = (
            lines
            | 'ScrapingZipFile' >> (beam.FlatMap(lambda x : scraper.get_zipfile_links(requests,jrdb_auth_info )))
            | 'RetrieveData' >> (beam.FlatMap(lambda x : scraper.expand_zip2bytearray(x, jrdb_auth_info)))
            | 'FileToRecordSequence' >> (beam.FlatMap(lambda x: file_to_recordset(x,characteristics[0].record_length)))
            | 'MapToColumns' >> (beam.Map(lambda x: map_to_record(x, characteristics)))

        )
        if location == "local":
            records | WriteToText("#local\\out\\testx_{table_name}.txt".format(table_name=table_name))
        else:
            if truncate:
                records | WriteToBigQuery(table_name, dataset_name, "yu-it-base"
                                          , write_disposition=BigQueryDisposition.WRITE_TRUNCATE
                                          )
                pass
            else:
                records | WriteToBigQuery(table_name, dataset_name, "yu-it-base")
                pass

    pass

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    dataset_name="jrdb_raw_data"
    from_date = "20180504"
    to_date = "20180604"


    #filter_terms = p | beam.io.ReadFromText("gs://deep_impact/assets/jrdb/auth_info.txt")
    print "注意:kza(マスタ)は全量取り込み時しか取り込みされません"
    run(dataset_name, "a_bac", from_date, to_date)
    #