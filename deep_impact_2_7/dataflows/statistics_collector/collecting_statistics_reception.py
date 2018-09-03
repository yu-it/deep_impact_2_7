# -*- coding:utf-8 -*-
import logging
from apache_beam.options.pipeline_options import PipelineOptions
import deep_impact_2_7.dataflows.statistics_collector.collecting_statistics_processpr as processor
#table,from,to
class AnalysisOption(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--location',
            default="local",
            help='localとするとファイル出力')
        parser.add_argument(
            '--table_name',
            required=True,
            help='分析するテーブル。a_bac,など')
        parser.add_argument(
            '--dataset_name',
            default='jrdb_raw_data',
            help='データセット')
        parser.add_argument(
            '--result_dataset',
            default='',
            help='結果データセット')
        parser.add_argument(
            '--result_table_name',
            default='',
            help='結果テーブル')
        #
        """
        parser.add_argument(
            '--runner',
            default='DirectRunner',
            help='取り込み終了日')
        parser.add_argument(
            '--project=yu-it-base',
            default='yu-it-base',
            help='取り込み終了日')
        parser.add_argument(
            '--staging_location',
            default='gs://yu-it-base-temp/dataflow/staging',
            help='取り込み終了日')
        parser.add_argument(
            '--temp_location',
            default='gs://yu-it-base-temp/dataflow/temp',
            help='取り込み終了日')
        parser.add_argument(
            '--setup_file',
            default='C:\github\deep_impact_2_7\setup.py',
            help='取り込み終了日')
        parser.add_argument(
            '--job_name',
            default='default_job_name',
            help='取り込み終了日')
"""

    def __init__(self,list):
        if not '--job_name' in list:
            load_index = -1
            for idx, item in enumerate(list):
                if item == '--table_name':
                    load_index = idx + 1

            list.append('--job_name')
            table_name = "default"
            from_date = "00000000"
            to_date = "99991231"
            if load_index >= 0:
                table_name = list[load_index]
            list.append('collect_statistics-{table_name}'.format(table_name=table_name.replace("_", "-")))
        super(AnalysisOption,self).__init__(list)


pipeline_options = AnalysisOption(['--runner','DirectRunner','--location', 'bq','--table_name', 'a_bac','--result_dataset', 'jrdb_raw_data_schema_info','--result_table_name', 'a_numisint',
                                   "--project","yu-it-base",'--staging_location','gs://yu-it-base-temp/dataflow/staging','--temp_location','gs://yu-it-base-temp/dataflow/temp','--setup_file','C:\github\deep_impact_2_7\setup.py'])
logging.getLogger().setLevel(logging.INFO)

processor.run(pipeline_options)
pass

