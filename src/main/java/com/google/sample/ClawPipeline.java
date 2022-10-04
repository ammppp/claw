package com.google.sample;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.ForwardingRule;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * Dataflow pipeline to pull logs from a PubSub topic, enhance logs, and write to S3
 */
public class ClawPipeline {

    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(ClawPipeline.class);

    // Cache of targetIPs for forwarding rules
    private static Map<String,String> targetIpMap = new TreeMap<String,String>();

    public static void main(String[] args) throws IOException {

        ClawOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ClawOptions.class);

        AwsBasicCredentials awsCredentials
            = AwsBasicCredentials.create(options.getAwsAccessKey(), options.getAwsSecretKey());
        options.setAwsCredentialsProvider(StaticCredentialsProvider.create(awsCredentials));

        Pipeline p = Pipeline.create(options);

        p.apply("Read Logs from PubSub", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
         .apply("Enhance Logs", getEnhancer(options.getProject()))
         .apply("Window Logs", Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
         .apply("Write Logs to S3",
                TextIO.write()
                    .withWindowedWrites()
                    .withNumShards(1)
                    .to(new MyFileNamePolicy(options.getS3OutputPrefix()))
                    .withTempDirectory(FileBasedSink.convertToFileResourceIfPossible(options.getS3TempDir())));

        p.run().waitUntilFinish();
    }

    /**
     * Returns the enhancer ParDo that augments log lines
     */
    private static ParDo.SingleOutput<String, String> getEnhancer(String gcpProject) {
        return  ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String word, OutputReceiver<String> out) throws Exception {
                Gson gson = new Gson();
                Map map = gson.fromJson(word, Map.class);

                String projectId =
                        (String)((Map)((Map)map.get("resource"))
                                .get("labels")).get("project_id");
                String forwardingRule =
                        (String)((Map)((Map)map.get("resource"))
                                .get("labels")).get("forwarding_rule_name");

                String targetIp = getTargetIp(projectId, forwardingRule);

                ((Map)map.get("httpRequest")).put("targetIp", targetIp);
                ((Map)map.get("httpRequest")).put("targetPort", 443);
                ((Map)map.get("jsonPayload")).put("targetIp", targetIp);
                ((Map)map.get("jsonPayload")).put("targetPort", 443);

                out.output(gson.toJson(map));
            }});

    }

    /**
     * Filename policy that appends window start yyyy-MM-dd_HH-mm suffix to output file
     */
    private static class MyFileNamePolicy extends FileBasedSink.FilenamePolicy {

        private transient DateTimeFormatter timestampFormatter;
        private String output;

        public MyFileNamePolicy(String output) {
            LOG.info("MyFileNamePolicy()");
            this.output = output;
        }

        private DateTimeFormatter getTimestampFormatter() {
            if(timestampFormatter == null) {
                timestampFormatter = DateTimeFormat.forPattern("yyy-MM-dd_HH-mm").withZoneUTC();
            }
            return timestampFormatter;
        }

        @Override
        public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
            LOG.info("Getting windowedFilename. Window: "+window.toString()+ " Pane: " + paneInfo.toString());
            return FileBasedSink.convertToFileResourceIfPossible(output
                    +getTimestampFormatter().print( ((IntervalWindow)window).start() ));
        }

        @Override
        public @Nullable ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Uses Google APIs to get the target IP address for a given forwarding rule
     */
    private static String getTargetIp(String project, String forwardingRule) throws IOException, GeneralSecurityException {

        String key = project+forwardingRule;
        String targetIp = targetIpMap.get(key);

        if(targetIp == null) {
            GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
            if (credentials.createScopedRequired()) {
                credentials = credentials.createScoped(Arrays.asList("https://www.googleapis.com/auth/cloud-platform"));
            }
            Compute computeService =
                    new Compute.Builder(
                            GoogleNetHttpTransport.newTrustedTransport(),
                            GsonFactory.getDefaultInstance(),
                            new HttpCredentialsAdapter(credentials)
                    ).setApplicationName("TIC-Sender/1.0").build();

            Compute.GlobalForwardingRules.Get request =
                    computeService.globalForwardingRules().get(project, forwardingRule);
            ForwardingRule response = request.execute();
            targetIp = response.getIPAddress();
            targetIpMap.put(key, targetIp);
        }
        return targetIp;
    }

    /**
     * Beams options interface to specify the pipeline parameters on pipeline launch
     */
    public interface ClawOptions extends AwsOptions, GcpOptions, StreamingOptions {
        @Description("The Cloud Pub/Sub topic to read from.")
        @Validation.Required
        String getInputTopic();
        void setInputTopic(String value);

        @Description("Output file's window size in number of minutes.")
        @Default.Integer(15)
        Integer getWindowSize();
        void setWindowSize(Integer value);

        @Description("Path of the output file including its filename prefix.")
        @Validation.Required
        String getS3OutputPrefix();
        void setS3OutputPrefix(String value);

        @Description("Output temp directory.")
        @Validation.Required
        String getS3TempDir();
        void setS3TempDir(String value);

        @Description("AWS Access Key.")
        @Validation.Required
        String getAwsAccessKey();
        void setAwsAccessKey(String value);

        @Description("AWS Secret Key.")
        @Validation.Required
        String getAwsSecretKey();
        void setAwsSecretKey(String value);
    }
}