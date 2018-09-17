# -*- coding:utf-8 -*-

import deep_impact_2_7.bq.bq as bq
import deep_impact_2_7.util as util
import collections
import deep_impact_2_7.bq.categories as categories

#
"""
class statistics_collector(object):
    def __init__(self, *args):
        self.table_name = args[0]
        self.column_name = args[1]
        self.stat_name = args[2]
        self.num = args[3]
        self.val1 = args[4]
        self.val2 = args[5]
"""

statistics_collector = collections.namedtuple("statistics_collector", [
                        "table_name",
                        "column_name",
                        "column_type",
                        "stat_name",
                        "num",
                        "val1",
                        "val2",
                        "note"
])

def query(dataset,table, column, column_type, limit):
    column_type = categories.data_characteristics_type.exchange[column_type]

    for retry in range(5):
        util.info("{column} try.{num}".format(column = column, num = retry))
        pass
        try:
            ret = bq.selectFromBq(statistics_collector, sql.format(target_column=column, target_column_ambigous=column, column_type = column_type,
                                                          target_table_name=table, lim = limit))  # target_column_ambigous:キャストエラーが出ることあり

        except:
            util.debug("processing(retry) {table}.{col}".format(col=column, table=table))
            ret = bq.selectFromBq(statistics_collector, sql.format(target_column=column,column_type = column_type,
                                                          target_column_ambigous="safe_cast({column} as string)".format(column=column),
                                                          target_table_name=table, lim = limit))  # target_column_ambigous:キャストエラーが出ることあり
        if ret is not None and len(ret) > 0:
            return ret

    return ret

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
      row_number() over (order by freq_info.count desc) as num,
      safe_cast(freq_info.val as string) as val1,
      safe_cast(freq_info.count as string) as val2,
      freq_info.note as note
    ) structs
  from
    (
      select 
        struct
          (
            val as val,
            meaning as note,
            count(1) as count
          ) as freq_info
      from
        base_values v
        left outer join jrdb_raw_data_schema_info.category_mst mst on mst.table_name = '{target_table_name}' and mst.column_pysical_name = '{target_column}' and mst.category = safe_cast(v.val as string)
      group by val, meaning
      order by freq_info.count desc
      limit {lim}
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
            '{column_type}' as column_type,
            'all_count' as stat_name,
            0 as num,
            safe_cast(count(1) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'distinct' as stat_name,
            0 as num,
            safe_cast(count(distinct val) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'max' as stat_name,
            0 as num,
            safe_cast(max(val) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'min' as stat_name,
            0 as num,
            safe_cast(min(val) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'avg' as stat_name,
            0 as num,
            safe_cast(avg(val_num) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'stddev' as stat_name,
            0 as num,
            safe_cast(stddev(val_num) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'num_zero' as stat_name,
            0 as num,
            safe_cast(countif(val_num = 0) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'num_null' as stat_name,
            0 as num,
            safe_cast(countif(val is null or nullif(safe_cast(val as string),'a') =  '') as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'max_without_error' as stat_name,
            0 as num,
            safe_cast(max(val_without_error) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'min_without_error' as stat_name,
            0 as num,
            safe_cast(min(val_without_error) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'error_higher_count' as stat_name,
            0 as num,
            safe_cast(countif(error_val_higher is not null) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'error_higher_max' as stat_name,
            0 as num,
            safe_cast(max(error_val_higher) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'error_higher_min' as stat_name,
            0 as num,
            safe_cast(min(error_val_higher) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'error_lower_count' as stat_name,
            0 as num,
            safe_cast(countif(error_val_lower is not null) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'error_lower_max' as stat_name,
            0 as num,
            safe_cast(max(error_val_lower) as string) as val1,
            '' as val2,
            '' as note
          ),
          struct
          (
            '{target_table_name}' as table_name,
            '{target_column}' as column_name,
            '{column_type}' as column_type,
            'error_lower_min' as stat_name,
            0 as num,
            safe_cast(min(error_val_lower) as string) as val1,
            '' as val2,
            '' as note
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
  '{column_type}' as column_type,
  structs.stat_name,
  structs.num,
  structs.val1,
  structs.val2,
  structs.note
from 
  freq_info
union all
select 
  structs.table_name,
  structs.column_name,
  '{column_type}' as column_type,
  structs.stat_name,
  structs.num,
  structs.val1,
  structs.val2,
  '' as note
from 
  histogram_info
union all
select 
  structs.table_name,
  structs.column_name,
  '{column_type}' as column_type,
  structs.stat_name,
  structs.num,
  structs.val1,
  structs.val2,
  '' as note
from 
  histogram_info_without_error
"""
