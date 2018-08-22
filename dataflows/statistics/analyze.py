# -*- coding: utf-8 -*-
import codecs
from google.cloud import bigquery
sql = """
#standardsql
with base_values as (
  select 
    val as val, 
    val_num as val_num, 
    case 
      when 
        ifnull
          (
            safe_divide
              (
                abs
                  (
                    dev_val
                  ) 
                ,stddev_val
              )
            ,0
          ) 
          > 3 
        then 
          null 
        else 
          val_num 
        end as val_without_error,
    case 
      when 
        ifnull
          (
            safe_divide
              (
                 dev_val
                ,stddev_val
              )
            ,0
          ) 
          > 3 
        then 
          val_num 
        else 
          null
        end as error_val_higher,
    case 
      when 
        ifnull
          (
            safe_divide
              (
                 dev_val
                ,stddev_val
              )
            ,0
          ) 
          < -3 
        then 
          val_num  
        else 
          null
        end as error_val_lower
  from 
    (
      select
        val as val, 
        val_num as val_num,
        val_num - avg(val_num) over (partition by null) as dev_val,
        stddev(val_num) over (partition by null) as stddev_val
      from
        (
          select 
            {target_column} as val, 
            safe_cast({target_column_ambigous} as numeric) as val_num
          from
            `yu-it-base.jrdb_raw_data.{target_table_name}` as kyi 
        ) vals
    )
),
freq_info as (
  select 
    struct(
      '{target_table_name}' as table_name,
      '{target_column}' as column_name,
      'ranking' as stat_name,
      row_number() over (order by freq_info.count) as num,
      safe_cast(freq_info.val as string) as val1,
      safe_cast(freq_info.count as string) as val2
    ) structs
  from
    (
      select 
        struct
          (
            val as val,
            count(1) as count
          ) as freq_info
      from
        base_values
      group by val
      order by freq_info.count desc
      limit 10
    )
),
histogram_info as (
  select
    struct 
      (
        '{target_table_name}' as table_name,
        '{target_column}' as column_name,
        'ntile_avg' as stat_name,
        ntile_rank as num,
        safe_cast(avg(val_num) as string) as val1,
        '' as val2
      ) structs
  from
    (
      select
        ntile(10) over (order by val) ntile_rank,
        val_num
      from
        base_values
    )
  group by ntile_rank
  union all
  select
    struct 
      (
        '{target_table_name}' as table_name,
        '{target_column}' as column_name,
        'ntile_min' as stat_name,
        ntile_rank as num,
        safe_cast(min(val) as string) as val1,
        '' as val2
      ) structs
  from
    (
      select
        ntile(10) over (order by val) ntile_rank,
        val
      from
        base_values
    )
  group by ntile_rank
  union all
  select
    struct 
      (
        '{target_table_name}' as table_name,
        '{target_column}' as column_name,
        'ntile_max' as stat_name,
        ntile_rank as num,
        safe_cast(max(val) as string) as val1,
        '' as val2
      ) structs
  from
    (
      select
        ntile(10) over (order by val) ntile_rank,
        val
      from
        base_values
    )
  group by ntile_rank
),
histogram_info_without_error as (
  select
    struct 
      (
        '{target_table_name}' as table_name,
        '{target_column}' as column_name,
        'ntile_without_error_avg' as stat_name,
        ntile_rank as num,
        safe_cast(avg(val_num) as string) as val1,
        '' as val2
      ) structs
  from
    (
      select
        ntile(10) over (order by val) ntile_rank,
        val_num
      from
        base_values
    )
  group by ntile_rank
  union all
  select
    struct 
      (
        '{target_table_name}' as table_name,
        '{target_column}' as column_name,
        'ntile_without_error_min' as stat_name,
        ntile_rank as num,
        safe_cast(min(val) as string) as val1,
        '' as val2
      ) structs
  from
    (
      select
        ntile(10) over (order by val) ntile_rank,
        val
      from
        base_values
    )
  group by ntile_rank
  union all
  select
    struct 
      (
        '{target_table_name}' as table_name,
        '{target_column}' as column_name,
        'ntile_without_error_max' as stat_name,
        ntile_rank as num,
        safe_cast(max(val) as string) as val1,
        '' as val2
      ) structs
  from
    (
      select
        ntile(10) over (order by val_without_error) ntile_rank,
        val_without_error val
      from
        base_values
    )
  group by ntile_rank
)
select 
  * 
from 
  unnest
  (
    (
      select
        [
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'distinct' as stat_name,
            0 as num,
            safe_cast(count(distinct val) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'max' as stat_name,
            0 as num,
            safe_cast(max(val) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'min' as stat_name,
            0 as num,
            safe_cast(min(val) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'avg' as stat_name,
            0 as num,
            safe_cast(avg(val_num) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'stddev' as stat_name,
            0 as num,
            safe_cast(stddev(val_num) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'num_zero' as stat_name,
            0 as num,
            safe_cast(countif(val_num = 0) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'num_null' as stat_name,
            0 as num,
            safe_cast(countif(val is null or nullif(safe_cast(val as string),'a') =  '') as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'max_without_error' as stat_name,
            0 as num,
            safe_cast(max(val_without_error) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'min_without_error' as stat_name,
            0 as num,
            safe_cast(min(val_without_error) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'error_higher_count' as stat_name,
            0 as num,
            safe_cast(countif(error_val_higher is not null) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'error_higher_max' as stat_name,
            0 as num,
            safe_cast(max(error_val_higher) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'error_higher_min' as stat_name,
            0 as num,
            safe_cast(min(error_val_higher) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'error_lower_count' as stat_name,
            0 as num,
            safe_cast(countif(error_val_lower is not null) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'error_lower_max' as stat_name,
            0 as num,
            safe_cast(max(error_val_lower) as string) as val1,
            '' as val2
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            'error_lower_min' as stat_name,
            0 as num,
            safe_cast(min(error_val_lower) as string) as val1,
            '' as val2
          )
        ]
      from
        base_values
    )
  )
union all
select 
  structs.table_name,
  structs.column_name,
  structs.stat_name,
  structs.num,
  structs.val1,
  structs.val2
from 
  freq_info
union all
select 
  structs.table_name,
  structs.column_name,
  structs.stat_name,
  structs.num,
  structs.val1,
  structs.val2
from 
  histogram_info
union all
select 
  structs.table_name,
  structs.column_name,
  structs.stat_name,
  structs.num,
  structs.val1,
  structs.val2
from 
  histogram_info_without_error
"""
schema_definition = [
    {
        "table_name":"a_bac",
        "columns":
            [
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
                "crlf",
            ]
    },
    {
        "table_name":"a_kab",
        "columns":
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
                "crlf",
            ]
    },
    {
        "table_name":"a_kyi",
        "columns":
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
                "crlf"
            ]
    },
    {
        "table_name": "a_kza",
        "columns":
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
                "crlf",
            ]
    },
    {
        "table_name": "a_sed",
        "columns":
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
                "crlf",
            ]
    },
    {
        "table_name": "a_ukc",
        "columns":
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
                "crlf",
            ]
    }

]
client = bigquery.Client(project='yu-it-base')
with codecs.open(r"C:\github\analysissummary.txt", "w", 'utf-8') as ws:
    for table_schema in schema_definition:
        table = table_schema["table_name"]
#   飛ばしたいテーブルはここに書く
#    if table not in ["a_sed"]:
#        print "skip({table}".format(table= table)
#        continue
        with codecs.open(r"C:\github\analysis_{table}.txt".format(table=table),"w", 'utf-8') as w:
            for column in table_schema["columns"]:
                for round in range(5):
                    try:
                        current_q = sql.format(target_column=column, target_column_ambigous=column,
                                               target_table_name=table)  # target_column_ambigous:キャストエラーが出ることあり
                        query_job = client.run_sync_query(
                            current_q)  # API request - starts the query
                        query_job.run()
                    except:
                        print "processing(retry) {table}.{col}".format(col=column, table=table)
                        current_q = sql.format(target_column=column, target_column_ambigous="safe_cast({column} as string)".format(column=column),
                                               target_table_name=table)  # target_column_ambigous:キャストエラーが出ることあり
                        query_job = client.run_sync_query(
                            current_q)  # API request - starts the query
                        query_job.run()
                    print "{table}.{col}[round {round}] {rows} rows returned".format(col=column,table=table, round=round, rows = len(query_job.rows))

                    for row in query_job.rows:
                        record = ",".join([unicode(row[i]) for i in range(6)])
                        w.write(record)
                        w.write("\n")
                        ws.write(record)
                        ws.write("\n")
                    if len(query_job.rows) > 0:
                        break

