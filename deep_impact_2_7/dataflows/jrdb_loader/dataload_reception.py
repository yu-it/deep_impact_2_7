# -*- coding:utf-8 -*-
import logging
from apache_beam.options.pipeline_options import PipelineOptions
import deep_impact_2_7.dataflows.jrdb_loader.dataload_processor as processor
#table,from,to
class DataloadOption(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--location',
            default="bq",
            help='ロードするテーブル。a_bac,など')
        parser.add_value_provider_argument(
            '--table_name',
            required=True,
            help='ロードするテーブル。a_bac,など')
        parser.add_value_provider_argument(
            '--dataset_name',
            default='jrdb_raw_data',
            help='データセット')
        parser.add_value_provider_argument(
            '--from_date',
            default='',
            help='取り込み開始日')
        parser.add_value_provider_argument(
            '--to_date',
            default='',
            help='取り込み終了日')
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
            '--job_name',
            default='default_job_name',
            help='取り込み終了日')
        """


    def __init__(self,list):
        if not '--job_name' in list:
            load_index = -1
            from_index = -1
            to_index = -1
            for idx, item in enumerate(list):
                if item == '--table_name':
                    load_index = idx + 1
                if item == '--from_date':
                    from_index = idx + 1
                if item == '--to_date':
                    to_index = idx + 1

            list.append('--job_name')
            table_name = "default"
            from_date = "00000000"
            to_date = "99991231"
            if load_index >= 0:
                table_name = list[load_index]
            if from_index >= 0:
                from_date = list[from_index]
            if to_index >= 0:
                to_date = list[to_index]
            list.append('{table_name}-{fromdate}-to-{todate}'.format(table_name=table_name.replace("_", "-"), fromdate=from_date,
                                                         todate=to_date))
        super(DataloadOption,self).__init__(list)


pipeline_options = DataloadOption(['--runner','DirectRunner','--location', 'local','--table_name', 'a_bac','--from_date', '20180506','--to_date', '20180606',
                                   "--project","yu-it-base",'--staging_location','gs://yu-it-base-temp/dataflow/staging','--temp_location','gs://yu-it-base-temp/dataflow/temp','--setup_file','C:\github\deep_impact_2_7\setup.py'])
logging.getLogger().setLevel(logging.INFO)

processor.run(pipeline_options)
pass

