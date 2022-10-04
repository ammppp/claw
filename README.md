<pre>
export GOOGLE_APPLICATION_CREDENTIALS=/PATH/TO/GCP/CREDENTIALS.json

ARGS="  --runner='DataflowRunner'"
ARGS+=" --project='PROJECT'"
ARGS+=" --region='REGION'"
ARGS+=" --tempLocation='gs://BUCKET/tmp'"
ARGS+=" --inputTopic='projects/PROJECT/topics/TOPIC'"
ARGS+=" --awsRegion='REGION'"
ARGS+=" --s3OutputPrefix='s3://BUCKET/gcp_logs/load_balancer/log_'"
ARGS+=" --s3TempDir='s3://BUCKET'"
ARGS+=" --windowSize='1'"
ARGS+=" --awsAccessKey='ACCESS-KEY'"
ARGS+=" --awsSecretKey='SECRET-KEY'"

mvn compile exec:java -Dexec.mainClass="com.google.sample.ClawPipeline" -Dexec.args="$ARGS"
</pre>
