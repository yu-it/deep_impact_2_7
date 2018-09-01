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

import deep_impact_2_7.dataflows.jrdb_loader.jrdb_file_scraper as scraper
import datetime
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import WriteToText

from past.builtins import unicode

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from deep_impact_2_7.dataflows import bq

DataCharisticsQuery = """
#standardSQL
select sum(length) over (partition by null) + 2 record_length, c.* from jrdb_raw_data_schema_info.data_characteristics c where table_name = '{table_name}'order by seq
"""
def get_data_charistics(dataset_name, table_name):
    chars = bq.selectFromBq(DataCharisticsQuery.format(table_name=table_name))
    return {"characteristic": chars,
            "record_length": chars[0][0]}

def file_to_recordset(data_entry,record_length):
    records = [(data_entry[0],data_entry[1][idx * record_length : (1 + idx) * record_length]) for idx in range(len(data_entry[1]) / record_length)]
    return records


def special_translation(table_name,column_name, value):
    pass

def convert_to_datetime(column_value,type_detail):

    if len(column_value) == 4 and type_detail == u"日時":
        return str(datetime.datetime(int(column_value),1,1))

    if len(column_value) == 4 and type_detail == u"時刻":
        return str(datetime.datetime(1990, 1, 1,int(column_value[0:2]),int(column_value[2:4])))

    if len(column_value) == 8 and type_detail == u"日時":
        return str(datetime.datetime(int(column_value[0:4]),int(column_value[4:6]),int(column_value[6:8])))

def split_function(recordentry, charistics):
    derive_from = recordentry[0]
    recordstring = recordentry[1]
    record = {}
    for entry in charistics["characteristic"][0:-1]:
        column_value = str.decode(recordstring[entry[6] - 1:(entry[6] + entry[5]) - 1], "shift-jis").strip()
        if entry[8] in (u"日時", u"時刻"):
            column_value = convert_to_datetime(column_value,entry[8])

        record.update({entry[2]:column_value})

    record.update({"distributed_date":derive_from})
    return record

def get_last_distributed_date(table_name):
    from_date = bq.selectFromBq(
        """
        #standardsql
        select max(distributed_date) from jrdb_raw_data.{table_name}
        """.format(table_name=table_name))
    if from_date is None or len(from_date) == 0:
        from_date = "19900101"
    else:
        from_date = (from_date[0][0] + datetime.timedelta(days=1)).strftime('%Y%m%d')
    return from_date


def run(dataset_name, table_name, from_date, to_date):

    charistics = get_data_charistics(dataset_name, table_name)
    auth_data = bq.selectFromBq(
        """
        #standardsql
        select * from jrdb_raw_data_schema_info.auth_info
        """)[0]
    if from_date is None:
        from_date = get_last_distributed_date()
    if to_date is None:
        to_date = "99999999"
    parser = argparse.ArgumentParser()
    parser.add_argument('--output',
                        dest='output',
                        # CHANGE 1/5: The Google Cloud Storage path is required
                        # for outputting the results.
                        default="{dataset_name}.{table_name}".format(dataset_name=dataset_name,table_name=table_name),
                        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_args.extend([
        # CHANGE 2/5: (OPTIONAL) Change this to DataflowRunner to
        # run your pipeline on the Google Cloud Dataflow Service.
        #'--runner=DataFlowRunner',
        # CHANGE 3/5: Your project ID is required in order to run your pipeline on
        # the Google Cloud Dataflow Service.
        '--project=yu-it-base',
        # CHANGE 4/5: Your Google Cloud Storage path is required for staging local
        # files.
        '--staging_location=gs://yu-it-base-temp/dataflow/staging',
        # CHANGE 5/5: Your Google Cloud Storage path is required for temporary
        # files.
        '--temp_location=gs://yu-it-base-temp/dataflow/temp',
        '--job_name={table_name}-{fromdate}-to-{todate}'.format(table_name=table_name.replace("_","-"), fromdate= from_date, todate=to_date),
        '--setup_file=C:\github\deep_impact_2_7\setup.py'
    ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True


    requests = [(table_name, from_date, to_date)]
    with beam.Pipeline(options=pipeline_options) as p:
        lines = (p
                 | beam.Create(
                    requests))
        # Count the occurrences of each word.
        records = (
            lines
            | 'ScrapingZipFile' >> (beam.FlatMap(lambda x : scraper.get_zipfile_links(x[0], x[1], x[2],auth_data[0],auth_data[1])))
            | 'RetrieveData' >> (beam.FlatMap(lambda x : scraper.expand_zip2bytearray(x["args"][0], x["data"], x["args"][1], x["args"][2], auth_data[0], auth_data[1])))
            | 'FileToRecord' >> (beam.FlatMap(lambda x: file_to_recordset(x,charistics["record_length"])))
            | 'MapToRecord' >> (beam.Map(lambda x: split_function(x, charistics)))

        )
        #records | WriteToBigQuery(table_name,dataset_name,"yu-it-base")
        records | WriteToText("#local\\out\\test_{table_name}.txt".format(table_name=table_name))

    pass

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    dataset_name="jrdb_raw_data"
    table_name="a_bac"
    from_date = "20180104"
    to_date = "20180604"


#filter_terms = p | beam.io.ReadFromText("gs://deep_impact/assets/jrdb/auth_info.txt")


run(dataset_name, table_name, from_date, to_date)
