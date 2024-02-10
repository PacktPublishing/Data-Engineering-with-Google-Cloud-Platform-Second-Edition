gcloud dataflow flex-template run mysql-datastream-bq \
--template-file-gcs-location gs://dataflow-templates-us-central1/latest/flex/Cloud_Datastream_to_BigQuery \
--region us-central1 \
--num-workers 1 \
--temp-location gs://dataflow-staging-us-central1-822672288347/tmp \
--enable-streaming-engine \
--parameters inputFilePattern=gs://$PROJECT_NAME-datastream-output/datastream-output/.,\
gcsPubSubSubscription=projects/$PROJECT_NAME/subscriptions/datastream_notifs_subs,\
inputFileFormat=avro,\
outputStagingDatasetTemplate=datastream_output,\
outputDatasetTemplate=datastream_output,\
outputStagingTableNameTemplate={_metadata_table}_log,\
outputTableNameTemplate={_metadata_table},\
deadLetterQueueDirectory=gs://$PROJECT_NAME-datastream-output/datastream_dlq,\
applyMerge=true