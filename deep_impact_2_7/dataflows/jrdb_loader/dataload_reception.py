# -*- coding:utf-8 -*-
import sys
import logging
import os
from apache_beam.options.pipeline_options import PipelineOptions
import deep_impact_2_7.dataflows.jrdb_loader.dataload_processor as processor
#table,from,to


print "data_characteristic,jrdb_authに変更があったときは再度パイプラインをデプロイする必要があります。"

class DataloadOption(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--table_name',
            help='ロードするテーブル。a_bac,など')
        parser.add_value_provider_argument(
            '--from_date',
            help='取り込み開始日')
        parser.add_value_provider_argument(
            '--to_date',
            help='取り込み終了日')
#

logging.getLogger().setLevel(logging.INFO)
def get_instance(table_name):
    if table_name == "a_bac":
        return processor.a_bac_loader()
    if table_name == "a_kab":
        return processor.a_kab_loader()
    if table_name == "a_kza":
        return processor.a_kza_loader()
    if table_name == "a_kyi":
        return processor.a_kyi_loader()
    if table_name == "a_sed":
        return processor.a_sed_loader()
    if table_name == "a_ukc":
        return processor.a_ukc_loader()

if len(sys.argv) > 1:
    pipeline_options = DataloadOption(sys.argv)
    loader = get_instance(pipeline_options._flags[pipeline_options._flags.index("--table_name")+1])
    loader.is_local = False
    if os.name == "nt":
        mode = "deploy"
    loader.run(pipeline_options)

else:

    processor.location = "local"
    runner = "DirectRunner"

    pipeline_options = DataloadOption(
        ['--runner', runner, '--table_name', 'a_kyi', '--dataset_name', 'jrdb_raw_data', '--from_date', '20180506',
         '--to_date', '20180606',
         "--project", "yu-it-base", '--staging_location', 'gs://yu-it-base-temp/dataflow/staging', '--temp_location',
         'gs://yu-it-base-temp/dataflow/temp', '--setup_file', 'C:\github\deep_impact_2_7\setup.py',
         '--template_location', 'gs://deep_impact/dataflow/jrdb_loader'])
    loader = get_instance(pipeline_options._flags[pipeline_options._flags.index("--table_name")+1])
    loader.run(pipeline_options)


