python -m deep_impact_2_7.dataflows.jrdb_loader.dataload_reception \
--runner DataflowRunner \
--project yu-it-base \
--staging_location gs://yu-it-base-temp/dataflow/staging \
--temp_location gs://yu-it-base-temp/dataflow/temp \
--template_location gs://yu-it-base-temp/dataflow/templates/jrdb_loader
