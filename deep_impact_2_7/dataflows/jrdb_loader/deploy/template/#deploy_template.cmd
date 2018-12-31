call C:\Python27\venvs\beam\Scripts\activate.bat
cd C:\github\deep_impact_2_7
echo on
python -m deep_impact_2_7.dataflows.jrdb_loader.dataload_reception --table_name a_@template@ --runner DataflowRunner --project yu-it-base --staging_location gs://yu-it-base-temp/dataflow/staging --temp_location gs://yu-it-base-temp/dataflow/temp --template_location gs://deep_impact/dataflow/jrdb_@template@_loader --setup_file C:\github\deep_impact_2_7\setup.py
cd %~dp0
rem #--output gs://yu-it-base-temp/dataflow/free \
call deploy_@template@_metadata.cmd
timeout 30