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


schema_definition = {
    "a_bac":[
                "race_key_place_code",
                "race_key_year",
                "race_key_no",
                "race_key_day",
                "race_key_round",
                "date",
                "time_to_start",
                "race_condition_distance",
                "race_condition_track_grass_dart_etc",
                "race_condition_track_right_left",
                "race_condition_track_inner_outer",
                "race_condition_type",
                "race_condition_condition",
                "race_condition_mark",
                "race_condition_weight",
                "race_condition_grade",
                "race_name",
                "count1",
                "horse_count",
                "course",
                "held_class",
                "race_short_name",
                "race_name_9",
                "data_category",
                "prize_1st",
                "prize_2nd",
                "prize_3rd",
                "prize_4th",
                "prize_5th",
                "summary_prize_1st",
                "summary_prize_2nd",
                "sold_flag",
                "win5flag",
                "reserve",
                "distributed_date",
            ]
    ,"a_kab":
            [
                "held_key_place_code",
                "held_key_year",
                "held_key_no",
                "held_key_day",
                "date",
                "held_class",
                "day_of_week",
                "field_name",
                "weather_code",
                "grass_field_status_code",
                "grass_field_status_inner",
                "grass_field_status_middle",
                "grass_field_status_outer",
                "grass_field_fidderence",
                "linear_field_difference_great_inner",
                "linear_field_difference_inner",
                "linear_field_difference_middle",
                "linear_field_difference_outer",
                "linear_field_difference_great_outer",
                "dart_field_status_code",
                "dart_field_status_inner",
                "dart_field_status_middle",
                "dart_field_status_outer",
                "dart_field_difference",
                "data_category",
                "consecutive_victory_count",
                "grass_type",
                "grass_length",
                "pressure_transfer",
                "freezing_avoidance",
                "rain",
                "reserve",
                "distributed_date",
            ],
    "a_kyi":
            [
                "race_key_place_code",
                "race_key_year",
                "race_key_no",
                "race_key_day",
                "race_key_round",
                "horse_no",
                "register_no",
                "horse_name",
                "IDM",
                "jockey_index",
                "info_index",
                "reserve_1",
                "reserve_2",
                "reserve_3",
                "comprehension_index",
                "leg_status",
                "distance_apptitude",
                "uptone_index",
                "rotation",
                "basis_odds",
                "basis_reputation_order",
                "basis_multiple_odds",
                "basis_multiple_reputation_order",
                "specific_info_white_double_circle",
                "specific_info_white_circle",
                "specific_info_black_triangle",
                "specific_info_white_triangle",
                "specific_info_cross",
                "comprehension_info_double_circle",
                "comprehension_info_circle",
                "comprehension_info_black_triange",
                "comprehension_info_white_triange",
                "comprehension_info_cross",
                "reputation_index",
                "torture_index",
                "stable_index",
                "torture_arrow_code",
                "stable_reputation_code",
                "jockey_anticipated_serial_rate",
                "passion_index",
                "hoof",
                "weight_apptitude_code",
                "class_code",
                "reserve_4",
                "blinker",
                "jockey_name",
                "load_weight",
                "apprentice_type",
                "torturer_name",
                "torturer_belonging",
                "other_data_key_previouse_performance_key1",
                "other_data_key_previouse_performance_key2",
                "other_data_key_previouse_performance_key3",
                "other_data_key_previouse_performance_key4",
                "other_data_key_previouse_performance_key5",
                "other_data_key_previouse_race_key1",
                "other_data_key_previouse_race_key2",
                "other_data_key_previouse_race_key3",
                "other_data_key_previouse_race_key4",
                "other_data_key_previouse_race_key5",
                "gate_no",
                "reserve_5",
                "mark_code_comprehension",
                "mark_code_idm",
                "mark_code_info",
                "mark_code_jockey",
                "mark_code_stable",
                "mark_code_tourture",
                "mark_code_passion",
                "grass_apptitude_code",
                "dart_aptitude",
                "jockey_code",
                "torturer_code",
                "reserve_6",
                "prize_info_obtain_prize",
                "prize_info_profit",
                "prize_info_condition_class",
                "speculation_ten_index",
                "speculation_pace_index",
                "speculation_uptone_index",
                "speculation_position_index",
                "speculation_pace_speculation",
                "speculation_following_middle_order",
                "speculation_following_middle_difference",
                "speculation_following_middle_inner_outer",
                "speculation_following_3F_order",
                "speculation_following_3F_difference",
                "speculation_following_3F_inner_outer",
                "speculation_goal_order",
                "speculation_goal_difference",
                "speculation_goal_inner_outer",
                "speculation_pace_mark",
                "distance_apptitude2",
                "gate_weight",
                "gate_weight_delta",
                "invalid_flag",
                "sex",
                "owner_name",
                "ownersclub_code",
                "horse_mark_code",
                "passion_order",
                "ls_index_order",
                "ten_index_ranking",
                "pace_index_ranking",
                "uptone_index_ranking",
                "position_index_ranking",
                "jockey_anticipated_winning_rate",
                "jockey_anticipated_3rd_rate",
                "transfer_type",
                "running_type",
                "figure",
                "figure_impression1",
                "figure_impression2",
                "figure_impression3",
                "horse_memo1",
                "horse_memo2",
                "horse_memo3",
                "reference_start_index",
                "reference_rate_of_delay",
                "reference_previous_race",
                "reference_previous_race_jockey_code",
                "million_index",
                "million_mark",
                "demoted_flag",
                "passion_type",
                "suspended_reason_code",
                "flag",
                "apprenticeship_round_count",
                "apprenticeship_date",
                "apprenticeship_day_count",
                "grazing",
                "grazing_ranking",
                "stable_rank",
                "reserve_7",
                "distributed_date"
            ],
    "a_kza":
            [
                "jockey_code",
                "register_delete_flag",
                "register_delete_date",
                "jockey_name",
                "jockey_name_kana",
                "jockey_short_name",
                "belonging_code",
                "belonging_region",
                "birthday",
                "first_licence_date",
                "apprentice_type",
                "belonging_stable",
                "jockey_comment",
                "comment_date",
                "this_year_leading",
                "this_year_performance",
                "this_year_obstacle_performance",
                "this_year_special_race_performance",
                "this_year_multi_praze",
                "last_year_leading",
                "last_year_performance",
                "last_year_obstacle_performance",
                "last_year_special_race_performance",
                "last_year_multi_praze",
                "total_performance",
                "total_obstacle_performance",
                "data_date",
                "reserve",
                "distributed_date",
            ]
    ,"a_sed":
            [
                "race_key_place_code",
                "race_key_year",
                "race_key_no",
                "race_key_day",
                "race_key_round",
                "horse_no",
                "performance_key_register_no",
                "performance_key_date",
                "horse_name",
                "race_condition_distance",
                "race_condition_track_grass_dart_etc",
                "race_condition_track_right_left",
                "race_condition_track_inner_outer",
                "race_condition_field_status",
                "race_condition_type",
                "race_condition_condition",
                "race_condition_mark",
                "race_condition_weight",
                "race_condition_grade",
                "race_condition_race_name",
                "race_condition_num_of_horse",
                "race_condition_race_short_name",
                "horse_performance_order",
                "horse_performance_abnormal_class",
                "horse_performance_time",
                "horse_performance_load_weight",
                "horse_performance_jockey_name",
                "horse_performance_torturer_name",
                "horse_performance_decided_single_odds",
                "horse_performance_decided_reputation_order",
                "JRDB_idm",
                "JRDB_point",
                "JRDB_field_difference",
                "JRDB_pace",
                "JRDB_delay",
                "JRDB_position_catching",
                "JRDB_unfavorable",
                "JRDB_previous_unfavorable",
                "JRDB_bit_unfavorable",
                "JRDB_following_unfavorable",
                "JRDB_race",
                "JRDB_course_catching",
                "JRDB_uptone_code",
                "JRDB_class_code",
                "JRDB_horse_figure_code",
                "JRDB_atomosphere",
                "JRDB_race_pace",
                "JRDB_horse_pace",
                "JRDB_ten_index",
                "JRDB_uptone_index",
                "JRDB_pace_index",
                "JRDB_race_p_index",
                "JRDB_1st_2nd_horse_name",
                "JRDB_1st_2nd_time_difference",
                "JRDB_previous_3f_time",
                "JRDB_following_3f_time",
                "JRDB_memo",
                "reserve_1",
                "decided_multi_odds_lower",
                "odds_single_10oclock",
                "odds_multi_10oclock",
                "ranking_corner1",
                "ranking_corner2",
                "ranking_corner3",
                "ranking_corner4",
                "previous_3f_difference",
                "following_3f_difference",
                "jockey_code",
                "torturer_code",
                "horse_weight",
                "horse_weight_delta",
                "weather_code",
                "course",
                "race_leg_condition",
                "refunds_single",
                "refunds_multiple",
                "main_praze",
                "prize",
                "race_pace_fluence",
                "horse_pace_fluent",
                "fourth_corner_catching",
                "reserve_2",
                "distributed_date",
            ],
    "a_ukc":
            [
                "register_no",
                "horse_name",
                "sex",
                "color_code",
                "horse_mark_code",
                "pedigree_father_name",
                "pedigree_mother_name",
                "pedigree_parent_name",
                "birthday",
                "father_birth_year",
                "mother_birth_year",
                "parent_birth_year",
                "owner_name",
                "ownersclub_code",
                "producer_name",
                "native_region",
                "register_delete_flag",
                "data_date",
                "father_series_code",
                "parent_series_code",
                "reserve",
                "distributed_date",
            ]
    }
DataCharacteristicsQuery_column_pysical_name = 2
DataCharacteristicsQuery_type = 8
schema_definition = {
    "a_bac":[
                "date",
                "race_condition_distance",
            ]
    }

DataCharacteristicsQuery_length = 5
DataCharacteristicsQuery_start_position = 6
DataCharacteristicsQuery_allow_zero = 9
DataCharacteristicsQuery_illegal_value_definition = 10
DataCharacteristicsQuery_illegal_value_condition = 11
DataCharacteristicsQuery_original_translation = 12

#
"""
csvファイルとロード先テーブル名をもらってデータをロードする

1.テーブル名をキーに
　jrdb_raw_data_schema_info.data_charisticsをロードする。
2.data_charisticsの桁数定義と型定義に従いデータを変換する
3.ロードする
4,独自変換があれば行う

"""
def analyze_column(table_column, characteristics):
    table, column = table_column
    column_characteristics = None
    for c in characteristics:
        if c.column_pysical_name == column:
            column_characteristics = c

    if column_characteristics.type not in [
        categories.data_characteristics_type.real_value,
        categories.data_characteristics_type.category,
        categories.data_characteristics_type.datetime
    ]:
        return []
    if column_characteristics.type in [
        categories.data_characteristics_type.real_value,
        categories.data_characteristics_type.datetime
    ]:
        ret = dao_analyze_column.query("jrdb_raw_data", table, column, 10)
        return ret
    else:
        ret = dao_analyze_column.query("jrdb_raw_data", table, column, 300)
        return ret


def as_db_record(record):
    return record._asdict()


#def run(dataset_name, table,schema_info_dataset_name, schema_info_table):
def run(pipeline_parameters):

    util.info("{dataset}.{table} analyze start".format(
        dataset = pipeline_parameters.dataset_name,
        table = pipeline_parameters.table_name ,
        )
    )
    characteristics = bq_schema.get_data_charistics(pipeline_parameters.dataset_name, pipeline_parameters.table_name)

    pipeline_parameters.view_as(SetupOptions).save_main_session = True

    table_columns = [(pipeline_parameters.table_name, column) for column in schema_definition[pipeline_parameters.table_name]]

    with beam.Pipeline(options=pipeline_parameters) as p:
        lines = (p
                 | beam.Create(
                    table_columns))

        records = (
            lines
            | 'Analyze' >> (beam.FlatMap(lambda x : analyze_column(x, characteristics)))
            | 'AsDBRecord' >> (beam.Map(lambda x: as_db_record(x)))
        )
#        records | WriteToBigQuery(pipeline_options.result_table_name,pipeline_options.result_dataset,"yu-it-base",schema=bq_schema.as_table_schema_object(schema.statistics_collector)
#                                  ,create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=BigQueryDisposition.WRITE_TRUNCATE
#                                  )
        if pipeline_parameters.location == "local":
            records | WriteToText("C:\github\deep_impact_2_7\deep_impact_2_7\dataflows\statistics\#local\out\\analyzez_{table_name}.txt".format(table_name=pipeline_parameters.result_table_name))
        else:
            records | WriteToBigQuery(pipeline_parameters.result_table_name, pipeline_parameters.result_dataset, "yu-it-base"
                                      ,schema=bq_schema.as_simple_table_schema_expression(schema.statistics)
                                      , create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                      write_disposition=BigQueryDisposition.WRITE_TRUNCATE
                                      )


    pass

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    dataset_name="jrdb_raw_data"
    table_name="a_sed"
    statistics_dataset_name="jrdb_raw_data_schema_info"
    statistics_table_name="a_sed"


    #filter_terms = p | beam.io.ReadFromText("gs://deep_impact/assets/jrdb/auth_info.txt")
    run(dataset_name, "a_bac", "jrdb_raw_data_schema_info", "a_bac_statistics")
    #

