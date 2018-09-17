
a_bac = '''
[
    {"name":"race_key_place_code","type":"string", "mode":"nullable"},
    {"name":"race_key_year","type":"string", "mode":"nullable"},
    {"name":"race_key_no","type":"string", "mode":"nullable"},
    {"name":"race_key_day","type":"string", "mode":"nullable"},
    {"name":"race_key_round","type":"string", "mode":"nullable"},
    {"name":"date","type":"datetime", "mode":"nullable"},
    {"name":"time_to_start","type":"datetime", "mode":"nullable"},
    {"name":"race_condition_distance","type":"float64", "mode":"nullable"},
    {"name":"race_condition_track_grass_dart_etc","type":"string", "mode":"nullable"},
    {"name":"race_condition_track_right_left","type":"string", "mode":"nullable"},
    {"name":"race_condition_track_inner_outer","type":"string", "mode":"nullable"},
    {"name":"race_condition_type","type":"string", "mode":"nullable"},
    {"name":"race_condition_condition","type":"string", "mode":"nullable"},
    {"name":"race_condition_mark","type":"string", "mode":"nullable"},
    {"name":"race_condition_weight","type":"string", "mode":"nullable"},
    {"name":"race_condition_grade","type":"string", "mode":"nullable"},
    {"name":"race_name","type":"string", "mode":"nullable"},
    {"name":"count1","type":"string", "mode":"nullable"},
    {"name":"horse_count","type":"float64", "mode":"nullable"},
    {"name":"course","type":"string", "mode":"nullable"},
    {"name":"held_class","type":"string", "mode":"nullable"},
    {"name":"race_short_name","type":"string", "mode":"nullable"},
    {"name":"race_name_9","type":"string", "mode":"nullable"},
    {"name":"data_category","type":"string", "mode":"nullable"},
    {"name":"prize_1st","type":"float64", "mode":"nullable"},
    {"name":"prize_2nd","type":"float64", "mode":"nullable"},
    {"name":"prize_3rd","type":"float64", "mode":"nullable"},
    {"name":"prize_4th","type":"float64", "mode":"nullable"},
    {"name":"prize_5th","type":"float64", "mode":"nullable"},
    {"name":"summary_prize_1st","type":"float64", "mode":"nullable"},
    {"name":"summary_prize_2nd","type":"float64", "mode":"nullable"},
    {"name":"sold_flag","type":"string", "mode":"nullable"},
    {"name":"win5flag","type":"string", "mode":"nullable"},
    {"name":"reserve","type":"string", "mode":"nullable"},
    {"name":"partitioning_date","type":"date", "mode":"nullable"},
    {"name":"distributed_date","type":"datetime", "mode":"nullable"}
]

'''
a_kab = '''
[
    {"name":"held_key_place_code","type":"string", "mode":"nullable"},
    {"name":"held_key_year","type":"string", "mode":"nullable"},
    {"name":"held_key_no","type":"string", "mode":"nullable"},
    {"name":"held_key_day","type":"string", "mode":"nullable"},
    {"name":"date","type":"datetime", "mode":"nullable"},
    {"name":"held_class","type":"string", "mode":"nullable"},
    {"name":"day_of_week","type":"string", "mode":"nullable"},
    {"name":"field_name","type":"string", "mode":"nullable"},
    {"name":"weather_code","type":"string", "mode":"nullable"},
    {"name":"grass_field_status_code","type":"string", "mode":"nullable"},
    {"name":"grass_field_status_inner","type":"string", "mode":"nullable"},
    {"name":"grass_field_status_middle","type":"string", "mode":"nullable"},
    {"name":"grass_field_status_outer","type":"string", "mode":"nullable"},
    {"name":"grass_field_fidderence","type":"float64", "mode":"nullable"},
    {"name":"linear_field_difference_great_inner","type":"float64", "mode":"nullable"},
    {"name":"linear_field_difference_inner","type":"float64", "mode":"nullable"},
    {"name":"linear_field_difference_middle","type":"float64", "mode":"nullable"},
    {"name":"linear_field_difference_outer","type":"float64", "mode":"nullable"},
    {"name":"linear_field_difference_great_outer","type":"float64", "mode":"nullable"},
    {"name":"dart_field_status_code","type":"string", "mode":"nullable"},
    {"name":"dart_field_status_inner","type":"string", "mode":"nullable"},
    {"name":"dart_field_status_middle","type":"string", "mode":"nullable"},
    {"name":"dart_field_status_outer","type":"string", "mode":"nullable"},
    {"name":"dart_field_difference","type":"float64", "mode":"nullable"},
    {"name":"data_category","type":"string", "mode":"nullable"},
    {"name":"consecutive_victory_count","type":"float64", "mode":"nullable"},
    {"name":"grass_type","type":"string", "mode":"nullable"},
    {"name":"grass_length","type":"float64", "mode":"nullable"},
    {"name":"pressure_transfer","type":"string", "mode":"nullable"},
    {"name":"freezing_avoidance","type":"string", "mode":"nullable"},
    {"name":"rain","type":"float64", "mode":"nullable"},
    {"name":"reserve","type":"string", "mode":"nullable"},
    {"name":"partitioning_date","type":"date", "mode":"nullable"},
    {"name":"distributed_date","type":"datetime", "mode":"nullable"}
]

'''
a_kyi = '''
[
    {"name":"race_key_place_code","type":"string", "mode":"nullable"},
    {"name":"race_key_year","type":"string", "mode":"nullable"},
    {"name":"race_key_no","type":"string", "mode":"nullable"},
    {"name":"race_key_day","type":"string", "mode":"nullable"},
    {"name":"race_key_round","type":"string", "mode":"nullable"},
    {"name":"horse_no","type":"string", "mode":"nullable"},
    {"name":"register_no","type":"string", "mode":"nullable"},
    {"name":"horse_name","type":"string", "mode":"nullable"},
    {"name":"IDM","type":"float64", "mode":"nullable"},
    {"name":"jockey_index","type":"float64", "mode":"nullable"},
    {"name":"info_index","type":"float64", "mode":"nullable"},
    {"name":"reserve_1","type":"string", "mode":"nullable"},
    {"name":"reserve_2","type":"string", "mode":"nullable"},
    {"name":"reserve_3","type":"string", "mode":"nullable"},
    {"name":"comprehension_index","type":"float64", "mode":"nullable"},
    {"name":"leg_status","type":"string", "mode":"nullable"},
    {"name":"distance_apptitude","type":"string", "mode":"nullable"},
    {"name":"uptone_index","type":"string", "mode":"nullable"},
    {"name":"rotation","type":"float64", "mode":"nullable"},
    {"name":"basis_odds","type":"float64", "mode":"nullable"},
    {"name":"basis_reputation_order","type":"float64", "mode":"nullable"},
    {"name":"basis_multiple_odds","type":"float64", "mode":"nullable"},
    {"name":"basis_multiple_reputation_order","type":"float64", "mode":"nullable"},
    {"name":"specific_info_white_double_circle","type":"float64", "mode":"nullable"},
    {"name":"specific_info_white_circle","type":"float64", "mode":"nullable"},
    {"name":"specific_info_black_triangle","type":"float64", "mode":"nullable"},
    {"name":"specific_info_white_triangle","type":"float64", "mode":"nullable"},
    {"name":"specific_info_cross","type":"float64", "mode":"nullable"},
    {"name":"comprehension_info_double_circle","type":"float64", "mode":"nullable"},
    {"name":"comprehension_info_circle","type":"float64", "mode":"nullable"},
    {"name":"comprehension_info_black_triange","type":"float64", "mode":"nullable"},
    {"name":"comprehension_info_white_triange","type":"float64", "mode":"nullable"},
    {"name":"comprehension_info_cross","type":"float64", "mode":"nullable"},
    {"name":"reputation_index","type":"float64", "mode":"nullable"},
    {"name":"torture_index","type":"float64", "mode":"nullable"},
    {"name":"stable_index","type":"float64", "mode":"nullable"},
    {"name":"torture_arrow_code","type":"string", "mode":"nullable"},
    {"name":"stable_reputation_code","type":"string", "mode":"nullable"},
    {"name":"jockey_anticipated_serial_rate","type":"float64", "mode":"nullable"},
    {"name":"passion_index","type":"float64", "mode":"nullable"},
    {"name":"hoof","type":"string", "mode":"nullable"},
    {"name":"weight_apptitude_code","type":"string", "mode":"nullable"},
    {"name":"class_code","type":"string", "mode":"nullable"},
    {"name":"reserve_4","type":"string", "mode":"nullable"},
    {"name":"blinker","type":"string", "mode":"nullable"},
    {"name":"jockey_name","type":"string", "mode":"nullable"},
    {"name":"load_weight","type":"float64", "mode":"nullable"},
    {"name":"apprentice_type","type":"string", "mode":"nullable"},
    {"name":"torturer_name","type":"string", "mode":"nullable"},
    {"name":"torturer_belonging","type":"string", "mode":"nullable"},
    {"name":"other_data_key_previouse_performance_key1","type":"string", "mode":"nullable"},
    {"name":"other_data_key_previouse_performance_key2","type":"string", "mode":"nullable"},
    {"name":"other_data_key_previouse_performance_key3","type":"string", "mode":"nullable"},
    {"name":"other_data_key_previouse_performance_key4","type":"string", "mode":"nullable"},
    {"name":"other_data_key_previouse_performance_key5","type":"string", "mode":"nullable"},
    {"name":"other_data_key_previouse_race_key1","type":"string", "mode":"nullable"},
    {"name":"other_data_key_previouse_race_key2","type":"string", "mode":"nullable"},
    {"name":"other_data_key_previouse_race_key3","type":"string", "mode":"nullable"},
    {"name":"other_data_key_previouse_race_key4","type":"string", "mode":"nullable"},
    {"name":"other_data_key_previouse_race_key5","type":"string", "mode":"nullable"},
    {"name":"gate_no","type":"float64", "mode":"nullable"},
    {"name":"reserve_5","type":"string", "mode":"nullable"},
    {"name":"mark_code_comprehension","type":"string", "mode":"nullable"},
    {"name":"mark_code_idm","type":"string", "mode":"nullable"},
    {"name":"mark_code_info","type":"string", "mode":"nullable"},
    {"name":"mark_code_jockey","type":"string", "mode":"nullable"},
    {"name":"mark_code_stable","type":"string", "mode":"nullable"},
    {"name":"mark_code_tourture","type":"string", "mode":"nullable"},
    {"name":"mark_code_passion","type":"string", "mode":"nullable"},
    {"name":"grass_apptitude_code","type":"string", "mode":"nullable"},
    {"name":"dart_aptitude","type":"string", "mode":"nullable"},
    {"name":"jockey_code","type":"string", "mode":"nullable"},
    {"name":"torturer_code","type":"string", "mode":"nullable"},
    {"name":"reserve_6","type":"string", "mode":"nullable"},
    {"name":"prize_info_obtain_prize","type":"float64", "mode":"nullable"},
    {"name":"prize_info_profit","type":"float64", "mode":"nullable"},
    {"name":"prize_info_condition_class","type":"string", "mode":"nullable"},
    {"name":"speculation_ten_index","type":"float64", "mode":"nullable"},
    {"name":"speculation_pace_index","type":"float64", "mode":"nullable"},
    {"name":"speculation_uptone_index","type":"float64", "mode":"nullable"},
    {"name":"speculation_position_index","type":"float64", "mode":"nullable"},
    {"name":"speculation_pace_speculation","type":"string", "mode":"nullable"},
    {"name":"speculation_following_middle_order","type":"float64", "mode":"nullable"},
    {"name":"speculation_following_middle_difference","type":"float64", "mode":"nullable"},
    {"name":"speculation_following_middle_inner_outer","type":"string", "mode":"nullable"},
    {"name":"speculation_following_3F_order","type":"float64", "mode":"nullable"},
    {"name":"speculation_following_3F_difference","type":"float64", "mode":"nullable"},
    {"name":"speculation_following_3F_inner_outer","type":"string", "mode":"nullable"},
    {"name":"speculation_goal_order","type":"float64", "mode":"nullable"},
    {"name":"speculation_goal_difference","type":"float64", "mode":"nullable"},
    {"name":"speculation_goal_inner_outer","type":"string", "mode":"nullable"},
    {"name":"speculation_pace_mark","type":"string", "mode":"nullable"},
    {"name":"distance_apptitude2","type":"string", "mode":"nullable"},
    {"name":"gate_weight","type":"string", "mode":"nullable"},
    {"name":"gate_weight_delta","type":"string", "mode":"nullable"},
    {"name":"invalid_flag","type":"string", "mode":"nullable"},
    {"name":"sex","type":"string", "mode":"nullable"},
    {"name":"owner_name","type":"string", "mode":"nullable"},
    {"name":"ownersclub_code","type":"string", "mode":"nullable"},
    {"name":"horse_mark_code","type":"string", "mode":"nullable"},
    {"name":"passion_order","type":"float64", "mode":"nullable"},
    {"name":"ls_index_order","type":"float64", "mode":"nullable"},
    {"name":"ten_index_ranking","type":"float64", "mode":"nullable"},
    {"name":"pace_index_ranking","type":"float64", "mode":"nullable"},
    {"name":"uptone_index_ranking","type":"float64", "mode":"nullable"},
    {"name":"position_index_ranking","type":"float64", "mode":"nullable"},
    {"name":"jockey_anticipated_winning_rate","type":"float64", "mode":"nullable"},
    {"name":"jockey_anticipated_3rd_rate","type":"float64", "mode":"nullable"},
    {"name":"transfer_type","type":"string", "mode":"nullable"},
    {"name":"running_type","type":"string", "mode":"nullable"},
    {"name":"figure","type":"string", "mode":"nullable"},
    {"name":"figure_impression1","type":"string", "mode":"nullable"},
    {"name":"figure_impression2","type":"string", "mode":"nullable"},
    {"name":"figure_impression3","type":"string", "mode":"nullable"},
    {"name":"horse_memo1","type":"string", "mode":"nullable"},
    {"name":"horse_memo2","type":"string", "mode":"nullable"},
    {"name":"horse_memo3","type":"string", "mode":"nullable"},
    {"name":"reference_start_index","type":"float64", "mode":"nullable"},
    {"name":"reference_rate_of_delay","type":"float64", "mode":"nullable"},
    {"name":"reference_previous_race","type":"string", "mode":"nullable"},
    {"name":"reference_previous_race_jockey_code","type":"string", "mode":"nullable"},
    {"name":"million_index","type":"float64", "mode":"nullable"},
    {"name":"million_mark","type":"string", "mode":"nullable"},
    {"name":"demoted_flag","type":"string", "mode":"nullable"},
    {"name":"passion_type","type":"string", "mode":"nullable"},
    {"name":"suspended_reason_code","type":"string", "mode":"nullable"},
    {"name":"flag","type":"string", "mode":"nullable"},
    {"name":"apprenticeship_round_count","type":"float64", "mode":"nullable"},
    {"name":"apprenticeship_date","type":"datetime", "mode":"nullable"},
    {"name":"apprenticeship_day_count","type":"float64", "mode":"nullable"},
    {"name":"grazing","type":"string", "mode":"nullable"},
    {"name":"grazing_ranking","type":"string", "mode":"nullable"},
    {"name":"stable_rank","type":"string", "mode":"nullable"},
    {"name":"reserve_7","type":"string", "mode":"nullable"},
    {"name":"partitioning_date","type":"date", "mode":"nullable"},
    {"name":"distributed_date","type":"datetime", "mode":"nullable"}
]

'''
a_kza = '''
[
    {"name":"jockey_code","type":"string", "mode":"nullable"},
    {"name":"register_delete_flag","type":"string", "mode":"nullable"},
    {"name":"register_delete_date","type":"datetime", "mode":"nullable"},
    {"name":"jockey_name","type":"string", "mode":"nullable"},
    {"name":"jockey_name_kana","type":"string", "mode":"nullable"},
    {"name":"jockey_short_name","type":"string", "mode":"nullable"},
    {"name":"belonging_code","type":"string", "mode":"nullable"},
    {"name":"belonging_region","type":"string", "mode":"nullable"},
    {"name":"birthday","type":"datetime", "mode":"nullable"},
    {"name":"first_licence_date","type":"datetime", "mode":"nullable"},
    {"name":"apprentice_type","type":"string", "mode":"nullable"},
    {"name":"belonging_stable","type":"string", "mode":"nullable"},
    {"name":"jockey_comment","type":"string", "mode":"nullable"},
    {"name":"comment_date","type":"datetime", "mode":"nullable"},
    {"name":"this_year_leading","type":"string", "mode":"nullable"},
    {"name":"this_year_performance","type":"string", "mode":"nullable"},
    {"name":"this_year_obstacle_performance","type":"string", "mode":"nullable"},
    {"name":"this_year_special_race_performance","type":"float64", "mode":"nullable"},
    {"name":"this_year_multi_praze","type":"float64", "mode":"nullable"},
    {"name":"last_year_leading","type":"string", "mode":"nullable"},
    {"name":"last_year_performance","type":"string", "mode":"nullable"},
    {"name":"last_year_obstacle_performance","type":"string", "mode":"nullable"},
    {"name":"last_year_special_race_performance","type":"float64", "mode":"nullable"},
    {"name":"last_year_multi_praze","type":"float64", "mode":"nullable"},
    {"name":"total_performance","type":"string", "mode":"nullable"},
    {"name":"total_obstacle_performance","type":"string", "mode":"nullable"},
    {"name":"data_date","type":"datetime", "mode":"nullable"},
    {"name":"reserve","type":"string", "mode":"nullable"},
    {"name":"partitioning_date","type":"date", "mode":"nullable"},
    {"name":"distributed_date","type":"datetime", "mode":"nullable"}
]

'''
a_sed = '''
[
    {"name":"race_key_place_code","type":"string", "mode":"nullable"},
    {"name":"race_key_year","type":"string", "mode":"nullable"},
    {"name":"race_key_no","type":"string", "mode":"nullable"},
    {"name":"race_key_day","type":"string", "mode":"nullable"},
    {"name":"race_key_round","type":"string", "mode":"nullable"},
    {"name":"horse_no","type":"string", "mode":"nullable"},
    {"name":"performance_key_register_no","type":"string", "mode":"nullable"},
    {"name":"performance_key_date","type":"datetime", "mode":"nullable"},
    {"name":"horse_name","type":"string", "mode":"nullable"},
    {"name":"race_condition_distance","type":"float64", "mode":"nullable"},
    {"name":"race_condition_track_grass_dart_etc","type":"string", "mode":"nullable"},
    {"name":"race_condition_track_right_left","type":"string", "mode":"nullable"},
    {"name":"race_condition_track_inner_outer","type":"string", "mode":"nullable"},
    {"name":"race_condition_field_status","type":"string", "mode":"nullable"},
    {"name":"race_condition_type","type":"string", "mode":"nullable"},
    {"name":"race_condition_condition","type":"string", "mode":"nullable"},
    {"name":"race_condition_mark","type":"string", "mode":"nullable"},
    {"name":"race_condition_weight","type":"string", "mode":"nullable"},
    {"name":"race_condition_grade","type":"string", "mode":"nullable"},
    {"name":"race_condition_race_name","type":"string", "mode":"nullable"},
    {"name":"race_condition_num_of_horse","type":"float64", "mode":"nullable"},
    {"name":"race_condition_race_short_name","type":"string", "mode":"nullable"},
    {"name":"horse_performance_order","type":"float64", "mode":"nullable"},
    {"name":"horse_performance_abnormal_class","type":"string", "mode":"nullable"},
    {"name":"horse_performance_time","type":"float64", "mode":"nullable"},
    {"name":"horse_performance_load_weight","type":"float64", "mode":"nullable"},
    {"name":"horse_performance_jockey_name","type":"string", "mode":"nullable"},
    {"name":"horse_performance_torturer_name","type":"string", "mode":"nullable"},
    {"name":"horse_performance_decided_single_odds","type":"float64", "mode":"nullable"},
    {"name":"horse_performance_decided_reputation_order","type":"float64", "mode":"nullable"},
    {"name":"JRDB_idm","type":"float64", "mode":"nullable"},
    {"name":"JRDB_point","type":"float64", "mode":"nullable"},
    {"name":"JRDB_field_difference","type":"float64", "mode":"nullable"},
    {"name":"JRDB_pace","type":"float64", "mode":"nullable"},
    {"name":"JRDB_delay","type":"float64", "mode":"nullable"},
    {"name":"JRDB_position_catching","type":"float64", "mode":"nullable"},
    {"name":"JRDB_unfavorable","type":"float64", "mode":"nullable"},
    {"name":"JRDB_previous_unfavorable","type":"float64", "mode":"nullable"},
    {"name":"JRDB_bit_unfavorable","type":"float64", "mode":"nullable"},
    {"name":"JRDB_following_unfavorable","type":"float64", "mode":"nullable"},
    {"name":"JRDB_race","type":"float64", "mode":"nullable"},
    {"name":"JRDB_course_catching","type":"string", "mode":"nullable"},
    {"name":"JRDB_uptone_code","type":"string", "mode":"nullable"},
    {"name":"JRDB_class_code","type":"string", "mode":"nullable"},
    {"name":"JRDB_horse_figure_code","type":"string", "mode":"nullable"},
    {"name":"JRDB_atomosphere","type":"string", "mode":"nullable"},
    {"name":"JRDB_race_pace","type":"string", "mode":"nullable"},
    {"name":"JRDB_horse_pace","type":"string", "mode":"nullable"},
    {"name":"JRDB_ten_index","type":"float64", "mode":"nullable"},
    {"name":"JRDB_uptone_index","type":"float64", "mode":"nullable"},
    {"name":"JRDB_pace_index","type":"float64", "mode":"nullable"},
    {"name":"JRDB_race_p_index","type":"float64", "mode":"nullable"},
    {"name":"JRDB_1st_2nd_horse_name","type":"string", "mode":"nullable"},
    {"name":"JRDB_1st_2nd_time_difference","type":"float64", "mode":"nullable"},
    {"name":"JRDB_previous_3f_time","type":"float64", "mode":"nullable"},
    {"name":"JRDB_following_3f_time","type":"float64", "mode":"nullable"},
    {"name":"JRDB_memo","type":"string", "mode":"nullable"},
    {"name":"reserve_1","type":"string", "mode":"nullable"},
    {"name":"decided_multi_odds_lower","type":"float64", "mode":"nullable"},
    {"name":"odds_single_10oclock","type":"float64", "mode":"nullable"},
    {"name":"odds_multi_10oclock","type":"float64", "mode":"nullable"},
    {"name":"ranking_corner1","type":"float64", "mode":"nullable"},
    {"name":"ranking_corner2","type":"float64", "mode":"nullable"},
    {"name":"ranking_corner3","type":"float64", "mode":"nullable"},
    {"name":"ranking_corner4","type":"float64", "mode":"nullable"},
    {"name":"previous_3f_difference","type":"float64", "mode":"nullable"},
    {"name":"following_3f_difference","type":"float64", "mode":"nullable"},
    {"name":"jockey_code","type":"string", "mode":"nullable"},
    {"name":"torturer_code","type":"string", "mode":"nullable"},
    {"name":"horse_weight","type":"float64", "mode":"nullable"},
    {"name":"horse_weight_delta","type":"float64", "mode":"nullable"},
    {"name":"weather_code","type":"string", "mode":"nullable"},
    {"name":"course","type":"string", "mode":"nullable"},
    {"name":"race_leg_condition","type":"string", "mode":"nullable"},
    {"name":"refunds_single","type":"float64", "mode":"nullable"},
    {"name":"refunds_multiple","type":"float64", "mode":"nullable"},
    {"name":"main_praze","type":"float64", "mode":"nullable"},
    {"name":"prize","type":"float64", "mode":"nullable"},
    {"name":"race_pace_fluence","type":"string", "mode":"nullable"},
    {"name":"horse_pace_fluent","type":"string", "mode":"nullable"},
    {"name":"fourth_corner_catching","type":"string", "mode":"nullable"},
    {"name":"reserve_2","type":"string", "mode":"nullable"},
    {"name":"partitioning_date","type":"date", "mode":"nullable"},
    {"name":"distributed_date","type":"datetime", "mode":"nullable"}
]

'''
a_tyb = '''
[
    {"name":"race_key_place_code","type":"string", "mode":"nullable"},
    {"name":"race_key_year","type":"string", "mode":"nullable"},
    {"name":"race_key_no","type":"string", "mode":"nullable"},
    {"name":"race_key_day","type":"string", "mode":"nullable"},
    {"name":"race_key_round","type":"string", "mode":"nullable"},
    {"name":"horse_no","type":"string", "mode":"nullable"},
    {"name":"IDM","type":"float64", "mode":"nullable"},
    {"name":"jockey_index","type":"float64", "mode":"nullable"},
    {"name":"info_index","type":"float64", "mode":"nullable"},
    {"name":"odds_index","type":"float64", "mode":"nullable"},
    {"name":"paddock_index","type":"float64", "mode":"nullable"},
    {"name":"reserve_1","type":"string", "mode":"nullable"},
    {"name":"comprehension_index","type":"float64", "mode":"nullable"},
    {"name":"gear_change_info","type":"string", "mode":"nullable"},
    {"name":"leg_info","type":"string", "mode":"nullable"},
    {"name":"invalid_flag","type":"string", "mode":"nullable"},
    {"name":"jockey_code","type":"string", "mode":"nullable"},
    {"name":"jockey_name","type":"string", "mode":"nullable"},
    {"name":"load_weight","type":"float64", "mode":"nullable"},
    {"name":"apprentice_type","type":"string", "mode":"nullable"},
    {"name":"field_status_code","type":"string", "mode":"nullable"},
    {"name":"weather_code","type":"string", "mode":"nullable"},
    {"name":"single_odds","type":"float64", "mode":"nullable"},
    {"name":"multiple_odds","type":"float64", "mode":"nullable"},
    {"name":"odds_time","type":"datetime", "mode":"nullable"},
    {"name":"horse_weight","type":"float64", "mode":"nullable"},
    {"name":"horse_weight_delta","type":"float64", "mode":"nullable"},
    {"name":"odds_mark","type":"string", "mode":"nullable"},
    {"name":"paddock_mark","type":"string", "mode":"nullable"},
    {"name":"just_before_info","type":"string", "mode":"nullable"},
    {"name":"reserve","type":"string", "mode":"nullable"},
    {"name":"partitioning_date","type":"date", "mode":"nullable"},
    {"name":"distributed_date","type":"datetime", "mode":"nullable"}]

'''
a_ukc = '''
[
    {"name":"register_no","type":"string", "mode":"nullable"},
    {"name":"horse_name","type":"string", "mode":"nullable"},
    {"name":"sex","type":"string", "mode":"nullable"},
    {"name":"color_code","type":"string", "mode":"nullable"},
    {"name":"horse_mark_code","type":"string", "mode":"nullable"},
    {"name":"pedigree_father_name","type":"string", "mode":"nullable"},
    {"name":"pedigree_mother_name","type":"string", "mode":"nullable"},
    {"name":"pedigree_parent_name","type":"string", "mode":"nullable"},
    {"name":"birthday","type":"datetime", "mode":"nullable"},
    {"name":"father_birth_year","type":"datetime", "mode":"nullable"},
    {"name":"mother_birth_year","type":"datetime", "mode":"nullable"},
    {"name":"parent_birth_year","type":"datetime", "mode":"nullable"},
    {"name":"owner_name","type":"string", "mode":"nullable"},
    {"name":"ownersclub_code","type":"string", "mode":"nullable"},
    {"name":"producer_name","type":"string", "mode":"nullable"},
    {"name":"native_region","type":"string", "mode":"nullable"},
    {"name":"register_delete_flag","type":"string", "mode":"nullable"},
    {"name":"data_date","type":"datetime", "mode":"nullable"},
    {"name":"father_series_code","type":"string", "mode":"nullable"},
    {"name":"parent_series_code","type":"string", "mode":"nullable"},
    {"name":"reserve","type":"string", "mode":"nullable"},
    {"name":"partitioning_date","type":"date", "mode":"nullable"},
    {"name":"distributed_date","type":"datetime", "mode":"nullable"}
]

'''