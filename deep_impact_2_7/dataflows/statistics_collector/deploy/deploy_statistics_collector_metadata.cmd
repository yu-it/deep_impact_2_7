cd %~dp0
cmd /c gsutil -o GSUtil:default_project_id=yu-it-base cp statistics_collector_metadata gs://deep_impact/dataflow/%1
