cd %~dp0
cmd /c gsutil -o GSUtil:default_project_id=yu-it-base cp jrdb_@template@_loader_metadata gs://deep_impact/dataflow/
