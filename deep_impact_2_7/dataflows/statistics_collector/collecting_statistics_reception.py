# -*- coding:utf-8 -*-
import sys
import logging
import os
from apache_beam.options.pipeline_options import PipelineOptions
import deep_impact_2_7.dataflows.statistics_collector.collecting_statistics_processor as processor
#table,from,to



class AnalysisOption(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--table_name',
            help='ロードするテーブル。a_bac,など')
        parser.add_argument(
            '--dataset_name',
            default='jrdb_raw_data',
            help='データセット名。デフォでjrdb_raw_data')
        parser.add_argument(
            '--result_table_name',
            help='結果テーブル')
        parser.add_argument(
            '--result_dataset',
            default='jrdb_raw_data_schema_info',
            help='結果データセット')

#

logging.getLogger().setLevel(logging.INFO)

if len(sys.argv) > 1:
    pipeline_options = AnalysisOption(sys.argv)
    loader = processor.collecting_statistics_processor()
    loader.is_local = False
    loader.run(pipeline_options)

else:

    processor.location = "local"
    runner = "DirectRunner"

    pipeline_options = AnalysisOption(
        ['--runner', runner, '--table_name', 'a_kyi', '--result_table_name', 'a_kab_stats',
         "--project", "yu-it-base", '--staging_location', 'gs://yu-it-base-temp/dataflow/staging', '--temp_location',
         'gs://yu-it-base-temp/dataflow/temp', '--setup_file', 'C:\github\deep_impact_2_7\setup.py',
         ])
    loader = processor.collecting_statistics_processor()
    loader.is_local = True
    loader.run(pipeline_options)


