cd ..\..
python -m dataflows.jrdb_loader.dataload_processor \
  --project "yu-it-base" \
  --runner DataflowRunner \
  --staging_location gs://yu-it-base-temp/dataflow/staging \
  --temp_location gs://yu-it-base-temp/dataflow/temp \
  --output $BUCKET/results/output

pause