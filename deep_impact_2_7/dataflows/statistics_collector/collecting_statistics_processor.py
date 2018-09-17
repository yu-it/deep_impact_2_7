# -*- coding: utf-8 -*-
import deep_impact_2_7.util as util
import deep_impact_2_7.bq.dao.analyze_column as dao_analyze_column
import deep_impact_2_7.bq.schemaUtil as bq_schema
import deep_impact_2_7.bq.categories as categories
import deep_impact_2_7.bq.layout.jrdb_raw_data_schema_info.schema as schema
import argparse
import logging
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import BigQueryDisposition
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import deep_impact_2_7.bq.dao.analyze_column



#
"""

"""
class collecting_statistics_processor(object):
    def __init__(self):
        self.is_local = False
    def analyze_column(self, dataset_name, column_characteristics):

        if column_characteristics.type not in [
            categories.data_characteristics_type.real_value,
            categories.data_characteristics_type.category,
            categories.data_characteristics_type.datetime,
            categories.data_characteristics_type.date
        ]:
            return []
        if column_characteristics.type in [
            categories.data_characteristics_type.real_value,
            categories.data_characteristics_type.datetime,
            categories.data_characteristics_type.date
        ]:
            max_bucket = 10
        else:
            max_bucket = 300
        return dao_analyze_column.query(dataset_name, column_characteristics.table_name, column_characteristics.column_pysical_name, column_characteristics.type, max_bucket)



    def as_db_record(self, record):
        return record._asdict()


    #def run(dataset_name, table,schema_info_dataset_name, schema_info_table):
    def run(self, pipeline_parameters):

        pipeline_parameters.view_as(SetupOptions).save_main_session = True

        with beam.Pipeline(options=pipeline_parameters) as p:
            lines = (p
                     | beam.Create(
                        [pipeline_parameters.table_name]))

            records = (
                lines
                | 'GetDataCharacteristics' >> (beam.FlatMap(lambda x : bq_schema.get_data_charistics(pipeline_parameters.dataset_name,x)))
                | 'Analyze' >> (beam.FlatMap(lambda x : self.analyze_column(pipeline_parameters.dataset_name, x)))
                | 'AsDBRecord' >> (beam.Map(lambda x: self.as_db_record(x)))
            )
    #        records | WriteToBigQuery(pipeline_options.result_table_name,pipeline_options.result_dataset,"yu-it-base",schema=bq_schema.as_table_schema_object(schema.statistics_collector)
    #                                  ,create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=BigQueryDisposition.WRITE_TRUNCATE
    #                                  )
            if self.is_local:
                records | WriteToText("#local\out\\analyzez_{table_name}.txt".format(table_name=pipeline_parameters.result_table_name))
            else:
#                records | WriteToBigQuery(pipeline_parameters.result_table_name, pipeline_parameters.result_dataset, "yu-it-base"
#                                          ,schema=bq_schema.as_simple_table_schema_expression(schema.statistics)
#                                          , create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
#                                          write_disposition=BigQueryDisposition.WRITE_TRUNCATE
#                                          )
                records | WriteToBigQuery(pipeline_parameters.result_table_name, pipeline_parameters.result_dataset, "yu-it-base"
                                          ,schema=bq_schema.as_simple_table_schema_expression(schema.statistics)
                                          )


        pass

