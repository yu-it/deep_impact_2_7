call C:\Python27\venvs\beam\Scripts\activate.bat
call :deploy a_kab
call :deploy a_bac
call :deploy a_kyi
call :deploy a_kza
call :deploy a_sed
call :deploy a_ukc

timeout 30
goto :eof




:deploy
cd C:\github\deep_impact_2_7
echo on
python -m deep_impact_2_7.dataflows.statistics_collector.collecting_statistics_reception --table_name %1 --dataset_name jrdb_raw_data --result_table_name %1_stats --result_dataset jrdb_raw_data_schema_info --runner DataflowRunner --project yu-it-base --staging_location gs://yu-it-base-temp/dataflow/staging --temp_location gs://yu-it-base-temp/dataflow/temp --template_location gs://deep_impact/dataflow/%1_statistics_collector --setup_file C:\github\deep_impact_2_7\setup.py
cd %~dp0
rem #--output gs://yu-it-base-temp/dataflow/free \
call deploy_statistics_collector_metadata.cmd %1_statistics_collector_metadata

:eof