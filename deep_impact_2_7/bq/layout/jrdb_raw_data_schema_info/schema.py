
a_bac_statistics = '''
[
    {"name":"table_name","type":"string", "mode":"nullable"},
    {"name":"column_name","type":"string", "mode":"nullable"},
    {"name":"stat_name","type":"string", "mode":"nullable"},
    {"name":"num","type":"integer", "mode":"nullable"},
    {"name":"val1","type":"string", "mode":"nullable"},
    {"name":"val2","type":"string", "mode":"nullable"}
]

'''
a_kab_statistics = '''
[
    {"name":"table_name","type":"string", "mode":"nullable"},
    {"name":"column_name","type":"string", "mode":"nullable"},
    {"name":"stat_name","type":"string", "mode":"nullable"},
    {"name":"num","type":"integer", "mode":"nullable"},
    {"name":"val1","type":"string", "mode":"nullable"},
    {"name":"val2","type":"string", "mode":"nullable"}
]

'''
a_kyi_statistics = '''
[
    {"name":"table_name","type":"string", "mode":"nullable"},
    {"name":"column_name","type":"string", "mode":"nullable"},
    {"name":"stat_name","type":"string", "mode":"nullable"},
    {"name":"num","type":"integer", "mode":"nullable"},
    {"name":"val1","type":"string", "mode":"nullable"},
    {"name":"val2","type":"string", "mode":"nullable"}
]

'''
a_kza_statistics = '''
[
    {"name":"table_name","type":"string", "mode":"nullable"},
    {"name":"column_name","type":"string", "mode":"nullable"},
    {"name":"stat_name","type":"string", "mode":"nullable"},
    {"name":"num","type":"integer", "mode":"nullable"},
    {"name":"val1","type":"string", "mode":"nullable"},
    {"name":"val2","type":"string", "mode":"nullable"}
]

'''
a_sed_statistics = '''
[
    {"name":"table_name","type":"string", "mode":"nullable"},
    {"name":"column_name","type":"string", "mode":"nullable"},
    {"name":"stat_name","type":"string", "mode":"nullable"},
    {"name":"num","type":"integer", "mode":"nullable"},
    {"name":"val1","type":"string", "mode":"nullable"},
    {"name":"val2","type":"string", "mode":"nullable"}
]

'''
a_ukc_statistics = '''
[
    {"name":"table_name","type":"string", "mode":"nullable"},
    {"name":"column_name","type":"string", "mode":"nullable"},
    {"name":"stat_name","type":"string", "mode":"nullable"},
    {"name":"num","type":"integer", "mode":"nullable"},
    {"name":"val1","type":"string", "mode":"nullable"},
    {"name":"val2","type":"string", "mode":"nullable"}
]

'''
category_mst = '''
[
    {"name":"table_name","type":"string", "mode":"nullable"},
    {"name":"column_pysical_name","type":"string", "mode":"nullable"},
    {"name":"column_logical_name","type":"string", "mode":"nullable"},
    {"name":"invalid_flag","type":"integer", "mode":"nullable"},
    {"name":"special_translation","type":"integer", "mode":"nullable"},
    {"name":"original_category","type":"integer", "mode":"nullable"},
    {"name":"category","type":"string", "mode":"nullable"},
    {"name":"meaning","type":"string", "mode":"nullable"},
    {"name":"minimum_class","type":"integer", "mode":"nullable"},
    {"name":"class_2","type":"integer", "mode":"nullable"},
    {"name":"class_3","type":"integer", "mode":"nullable"},
    {"name":"class_4","type":"integer", "mode":"nullable"},
    {"name":"class_5","type":"integer", "mode":"nullable"},
    {"name":"class_6","type":"integer", "mode":"nullable"},
    {"name":"class_7","type":"integer", "mode":"nullable"},
    {"name":"maximum_class","type":"integer", "mode":"nullable"},
    {"name":"minimum_class_name","type":"string", "mode":"nullable"},
    {"name":"class_name_2","type":"string", "mode":"nullable"},
    {"name":"class_name_3","type":"string", "mode":"nullable"},
    {"name":"class_name_4","type":"string", "mode":"nullable"},
    {"name":"class_name_5","type":"string", "mode":"nullable"},
    {"name":"class_name_6","type":"string", "mode":"nullable"},
    {"name":"class_name_7","type":"string", "mode":"nullable"},
    {"name":"maximum_class_name","type":"string", "mode":"nullable"},
    {"name":"strength","type":"integer", "mode":"nullable"},
    {"name":"note","type":"string", "mode":"nullable"},
    {"name":"valid_from","type":"date", "mode":"nullable"},
    {"name":"valid_to","type":"date", "mode":"nullable"}
]

'''
data_characteristics = '''
[
    {"name":"table_name","type":"string", "mode":"nullable"},
    {"name":"column_pysical_name","type":"string", "mode":"nullable"},
    {"name":"column_logical_name","type":"string", "mode":"nullable"},
    {"name":"seq","type":"integer", "mode":"nullable"},
    {"name":"length","type":"integer", "mode":"nullable"},
    {"name":"start_position","type":"integer", "mode":"nullable"},
    {"name":"note","type":"string", "mode":"nullable"},
    {"name":"type","type":"string", "mode":"nullable"},
    {"name":"allow_zero","type":"string", "mode":"nullable"},
    {"name":"illegal_value_definition","type":"string", "mode":"nullable"},
    {"name":"illegal_value_condition","type":"string", "mode":"nullable"},
    {"name":"original_translation","type":"string", "mode":"nullable"}]

'''
statistics = '''
[
    {"name":"table_name","type":"string", "mode":"nullable"},
    {"name":"column_name","type":"string", "mode":"nullable"},
    {"name":"column_type","type":"string", "mode":"nullable"},
    {"name":"stat_name","type":"string", "mode":"nullable"},
    {"name":"num","type":"integer", "mode":"nullable"},
    {"name":"val1","type":"string", "mode":"nullable"},
    {"name":"val2","type":"string", "mode":"nullable"},
    {"name":"note","type":"string", "mode":"nullable"}
]

'''
table_name = '''
[
    {"name":"column_name","type":"type", "mode":"nullable"}
]

'''