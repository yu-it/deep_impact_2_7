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



class jrdb_loader_parameter:
    def __init__(self, from_date,to_date):
        self.from_date = from_date
        self.to_date = to_date

class abstract_data_loader(object):
    def __init__ (self, dataset_name, table_name):
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.truncate = False
        self.characteristics = get_data_charistics(dataset_name, table_name)
        self.auth_data = get_auth_info.query()
        #self.max_from_date = self.get_last_distributed_date(table_name)
        self.is_local = True

    def get_last_distributed_date(self, table_name):
        from_date = dao_get_last_distributed_date.query(table_name)
        if from_date is None:
            from_date = "19900101"
        else:
            from_date = (from_date + datetime.timedelta(days=1)).strftime('%Y%m%d')
        return from_date

    def bytes_to_fixed_length_record_sequence(self, file_name, byte_seq, record_length):
        records = [(file_name,byte_seq[idx * record_length : (1 + idx) * record_length]) for idx in range(len(byte_seq) / record_length)]
        return records

    def special_translation(self, table_name,column_name, value):
        pass

    def convert_to_datetime(self, column_value,type_detail):

        if len(column_value) == 4 and type_detail == categories.data_characteristics_type.datetime:
            return str(datetime.datetime(int(column_value),1,1))

        if len(column_value) == 4 and type_detail == categories.data_characteristics_type.time:
            return str(datetime.datetime(1990, 1, 1,int(column_value[0:2]),int(column_value[2:4])))

        if len(column_value) == 8 and type_detail == categories.data_characteristics_type.datetime:
            return str(datetime.datetime(int(column_value[0:4]),int(column_value[4:6]),int(column_value[6:8])))
        if len(column_value) == 8 and type_detail == categories.data_characteristics_type.date:
            return str(datetime.date(int(column_value[0:4]),int(column_value[4:6]),int(column_value[6:8])))

    def map_to_record(self, recordentry, charistics):
        derive_from = recordentry[0]
        recordstring = recordentry[1]
        record = {}
        for idx,entry in enumerate(charistics[0:-1]):

            #util.debug("No{idx} position:{x}-{y}".format(
            #    idx = idx,
            #    x = entry.start_position - 1,
            #    y = (entry.start_position + entry.length) - 1))
            try:
                column_value = str.decode(
                    recordstring[
                        entry.start_position - 1
                        :
                        (entry.start_position + entry.length) - 1
                    ], "ms932"
                ).strip()
                if entry.type in (categories.data_characteristics_type.datetime, categories.data_characteristics_type.date, categories.data_characteristics_type.time):
                    column_value = self.convert_to_datetime(column_value,entry.type)
                if entry.original_translation <> u"":
                    column_value = eval(entry.original_translation,{"x":column_value,"recordstring":recordstring,"util":util,"record":record, "derive_from": derive_from})
                if (entry.allow_zero == u"0" or entry.allow_zero == "0") and column_value == "0":
                    column_value = ""
                if entry.illegal_value_condition <> u"" and eval(
                        entry.illegal_value_condition,{"x":column_value}):
                    column_value = ""

            except Exception as ex:
                column_value = None

            if entry.type in (
                    categories.data_characteristics_type.real_value) and (type(column_value) in (str,unicode)):
                column_value = column_value.replace(" ", "")
                column_value = column_value.replace(u" ", "")
                if column_value not in (u'', ''):
                    try:
                        float(column_value)
                    except:
                        import random
                        if random.random() < 0.000001:
                            util.info("detect {exname} at No{idx} position:{x}-{y},sequence:{seq}".format(exname=str(ex), idx = idx, x =entry[6] - 1, y =(entry[6] + entry[5]) - 1, seq=recordstring[entry[6] - 1:(entry[6] + entry[5]) - 1]))
                        return None


            if entry.type in (
                    categories.data_characteristics_type.real_value,
                    categories.data_characteristics_type.datetime,
                    categories.data_characteristics_type.date,
                    categories.data_characteristics_type.time) and column_value == u"":
                column_value = None

            record.update({entry.column_pysical_name:column_value})

        record.update({"distributed_date":derive_from})
        return record



    def init_param(self, from_date, to_date):
        from_date = from_date.get() if from_date.get() is not None else self.get_last_distributed_date(self.table_name)
        to_date = to_date.get() if to_date.get() is not None else '99999999'
        if from_date == "00000000":
            self.truncate = True
        util.info("load to {tab} from {from_date} to {to_date}, truncate {trunc}".format(tab = self.table_name, from_date = from_date, to_date = to_date,trunc = self.truncate))
        return jrdb_loader_parameter(
            from_date,
            to_date
        )
    def run(self, pipeline_parameter):

        google_cloud_options = pipeline_parameter.view_as(beam.options.pipeline_options.GoogleCloudOptions)

        param = jrdb_loader_parameter(pipeline_parameter.from_date, pipeline_parameter.to_date)
        util.debug("setup pipeline")
        with beam.Pipeline(options=pipeline_parameter) as p:
            lines = (p
                     | beam.Create(
                        [param]))
            # Count the occurrences of each word.
            #characteristics などをInitParamに入れた

            records = (
                lines
                | 'InitParam' >> (beam.Map(lambda x : self.init_param(x.from_date, x.to_date)))
                #| 'SetFromDateToDate' >> (beam.Map(lambda x : expand_params(x)))
                | 'ScrapingZipFileLink' >> (beam.FlatMap(lambda x : scraper.get_zipfile_links(self.table_name, x.from_date, x.to_date, self.auth_data)))
                | 'DownloadZipFileAndExtractDataNeeded' >> (beam.FlatMap(lambda x : scraper.download_zip_file_and_extract_data_needed(x.table_name, x.from_date, x.to_date, x.url, self.auth_data )))
                | 'BytesToFixedLengthRecordSequence' >> (beam.FlatMap(lambda x: self.bytes_to_fixed_length_record_sequence(x.file_name, x.byte_seq,self.characteristics[0].record_length)))
                | 'MapToColumns' >> (beam.Map(lambda x: self.map_to_record(x, self.characteristics)))
                | 'NullSkip' >> (beam.Filter(lambda x: x is not None))
            )
            util.debug("exec pipeline")
            if self.is_local:
                records | WriteToText("#local\out\\testxx_{table_name}.txt".format(table_name=self.table_name))
            else:
                if self.truncate:
                    records | WriteToBigQuery(self.table_name, self.dataset_name, "yu-it-base"
                                              , write_disposition=BigQueryDisposition.WRITE_TRUNCATE
                                              )
                    pass
                else:
                    records | WriteToBigQuery(self.table_name, self.dataset_name, "yu-it-base")
                    pass

        pass

class a_bac_loader(abstract_data_loader):
    def __init__(self):
        super(a_bac_loader, self).__init__("jrdb_raw_data", "a_bac")
class a_kab_loader(abstract_data_loader):
    def __init__(self):
        super(a_kab_loader, self).__init__("jrdb_raw_data", "a_kab")
class a_kza_loader(abstract_data_loader):
    def __init__(self):
        super(a_kza_loader, self).__init__("jrdb_raw_data", "a_kza")
class a_kyi_loader(abstract_data_loader):
    def __init__(self):
        super(a_kyi_loader, self).__init__("jrdb_raw_data", "a_kyi")
class a_sed_loader(abstract_data_loader):
    def __init__(self):
        super(a_sed_loader, self).__init__("jrdb_raw_data", "a_sed")
class a_ukc_loader(abstract_data_loader):
    def __init__(self):
        super(a_ukc_loader, self).__init__("jrdb_raw_data", "a_ukc")
class a_tyb_loader(abstract_data_loader):
    def __init__(self):
        super(a_tyb_loader, self).__init__("jrdb_raw_data", "a_tyb")