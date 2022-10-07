<pre>
# Pull down the sample code
git clone https://github.com/ammppp/claw.git
cd claw

# Set service account credentials that can launch a Dataflow job
export GOOGLE_APPLICATION_CREDENTIALS=/PATH/TO/GCP/CREDENTIALS.json

# Parameters specific to your GCP environment
ARGS="  --runner='DataflowRunner'"
ARGS+=" --project='PROJECT'"
ARGS+=" --region='REGION'"
ARGS+=" --tempLocation='gs://BUCKET/tmp'"
ARGS+=" --inputTopic='projects/PROJECT/topics/TOPIC'"

# Parameters specific to the AWS Claw environment
ARGS+=" --awsRegion='REGION'"
ARGS+=" --s3OutputPrefix='s3://BUCKET/gcp_logs/load_balancer/log_'"
ARGS+=" --s3TempDir='s3://BUCKET'"
ARGS+=" --awsAccessKey='ACCESS-KEY'"
ARGS+=" --awsSecretKey='SECRET-KEY'"

# Window interval in minutes for passing logs to CLAW
ARGS+=" --windowSize='1'"

# Build and launch the pipeline
mvn compile exec:java -Dexec.mainClass="com.google.sample.ClawPipeline" -Dexec.args="$ARGS"
</pre>
