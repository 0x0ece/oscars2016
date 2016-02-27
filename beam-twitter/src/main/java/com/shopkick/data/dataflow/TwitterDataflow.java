package com.shopkick.data.dataflow;

import com.dataartisans.flink.dataflow.FlinkPipelineRunner;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.io.UnboundedFlinkSource;
import com.dataartisans.flink.dataflow.FlinkPipelineOptions;
import com.dataartisans.flink.dataflow.io.KafkaIO;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.util.serialization.JavaDefaultStringSchema;

import java.util.Properties;

import org.json.JSONObject;
import org.json.JSONArray;

import org.joda.time.Duration;
import org.joda.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Twitter entities count pipeline
 */
public class TwitterDataflow {

  static final String INPUT = "tweets";  // Default kafka topic to read from
  static final String OUTPUT = "entities_count";  // Default kafka topic to read from
  static final String ZOOKEEPER = "localhost:2181";  // Default zookeeper to connect to for Kafka
  static final String KAFKA_BROKER = "localhost:9092";  // Default kafka broker to contact
  static final String KAFKA_GROUP_ID = "tweetsGroup";  // Default groupId
  static final long WINDOW_SIZE = 600;  // Default window duration in seconds
  static final long WINDOW_SLIDE = 10;  // Default window slide in seconds

  /** Extracts entities (hashtags and mentions) from a tweet.
      Input is a json string.
      Supports quoted_status, emitted with the quoting tweet timestamp.
      TODO: use jackson
  */
  static class ExtractEntitiesFn extends DoFn<String, String>{

    private static final Logger LOG = LoggerFactory.getLogger(ExtractEntitiesFn.class);

    @Override
    public void processElement(ProcessContext c) {

      String in = c.element();
      JSONObject tweet = new JSONObject(in);

      Instant timestamp = new Instant(tweet.getLong("timestamp_ms"));
      LOG.debug("Timestamp: " + timestamp);

      this.processTweet(c, tweet, timestamp);
      
      if (!tweet.isNull("quoted_status")) {
        LOG.debug("Found quoted_status");
        JSONObject quotedTweet = tweet.getJSONObject("quoted_status");
        this.processTweet(c, quotedTweet, timestamp);
      }
    }

    protected void processTweet(ProcessContext c, JSONObject tweet, Instant timestamp) {

      JSONObject entities = tweet.getJSONObject("entities");

      JSONArray mentions = entities.getJSONArray("user_mentions");
      for (int i = 0; i < mentions.length(); i++) {
        JSONObject mention = mentions.getJSONObject(i);
        LOG.debug("Mention: " + "@"+mention.getString("screen_name").toLowerCase());
        c.outputWithTimestamp("@"+mention.getString("screen_name").toLowerCase(), timestamp);
      }

      JSONArray hashtags = entities.getJSONArray("hashtags");
      for (int i = 0; i < hashtags.length(); i++) {
        JSONObject hashtag = hashtags.getJSONObject(i);
        LOG.debug("Hashtag: " + "#"+hashtag.getString("text").toLowerCase());
        c.outputWithTimestamp("#"+hashtag.getString("text").toLowerCase(), timestamp);
      }      
    }

    @Override
    public Duration getAllowedTimestampSkew(){
      return Duration.millis(Long.MAX_VALUE);
    }
  }

  /** Converts a windowed (Entity, Count) into a printable 
      comma separated string: window end, entity, count.
      TODO: change to SimpleFunction when support for Dataflow 1.4 is available.
  */
  public static class FormatAsTextFn extends DoFn<KV<String, Long>, String> {
    @Override
    public void processElement(ProcessContext c) {
      String text = ((IntervalWindow)c.window()).end() + "," + c.element().getKey() + "," + c.element().getValue();
      c.output(text);
    }
  }

  /** Converts a globally windowed (Entity, Count) into a printable
      comma separated string: entity, count.  
      TODO: change to SimpleFunction when support for Dataflow 1.4 is available.
  */
  public static class FormatAsTextGlobalFn extends DoFn<KV<String, Long>, String> {
    @Override
    public void processElement(ProcessContext c) {
      String text = c.element().getKey() + "," + c.element().getValue();
      c.output(text);
    }
  }

  /**
   * Options supported by {@link TwitterDataflow}.
   * Inherits standard configuration options, and flink-dataflow options.
   */
  public static interface TwitterDataflowOptions extends PipelineOptions, FlinkPipelineOptions {
    @Description("Kafka topic to read from")
    @Default.String(INPUT)
    String getInput();
    void setInput(String value);

    @Description("Kafka topic to write to (or, path of the file to write to)")
    @Default.String(OUTPUT)
    String getOutput();
    void setOutput(String value);

    @Description("The Zookeeper server to connect to")
    @Default.String(ZOOKEEPER)
    String getZookeeper();

    void setZookeeper(String value);

    @Description("The Kafka Broker to read from/write to")
    @Default.String(KAFKA_BROKER)
    String getKafkaBroker();

    void setKafkaBroker(String value);

    @Description("The groupId")
    @Default.String(KAFKA_GROUP_ID)
    String getKafkaGroup();

    void setKafkaGroup(String value);

    @Description("Sliding window duration, in seconds")
    @Default.Long(WINDOW_SIZE)
    Long getWindowSize();

    void setWindowSize(Long value);

    @Description("Window slide, in seconds")
    @Default.Long(WINDOW_SLIDE)
    Long getWindowSlide();

    void setWindowSlide(Long value);
  }

  public static void main(String[] args) {

    PipelineOptionsFactory.register(TwitterDataflowOptions.class);

    TwitterDataflowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(TwitterDataflowOptions.class);

    // Flink specific options
    options.setJobName("TwitterDataflow");
    options.setStreaming(true);
    options.setRunner(FlinkPipelineRunner.class);

    Pipeline pipeline = Pipeline.create(options);

    // Kafka options, shared among consumer and producer
    Properties kafkaProps = new Properties();
    kafkaProps.setProperty("zookeeper.connect", options.getZookeeper());
    kafkaProps.setProperty("bootstrap.servers", options.getKafkaBroker());
    kafkaProps.setProperty("group.id", options.getKafkaGroup());

    // Flink consumer that reads from a Kafka topic
    FlinkKafkaConsumer082 kafkaConsumer = new FlinkKafkaConsumer082<String>(
      options.getInput(),
      new SimpleStringSchema(), kafkaProps);

    // Flink producer that writes to a Kafka topic
    FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
      options.getOutput(),
      new JavaDefaultStringSchema(), kafkaProps, null);

    // This requires KafkaIO, check out https://github.com/ecesena/flink-dataflow
    // or use TextIO as in the comment below
    PTransform<PCollection<String>, PDone> outputFn =
        new KafkaIO.Write.Bound<String>("WriteEntityCounts", kafkaProducer);
        // TextIO.Write.named("WriteEntityCounts").to(options.getOutput());

    pipeline
      .apply("Input", Read.named("ReadTweets").from(new UnboundedFlinkSource<String, UnboundedSource.CheckpointMark>(options, kafkaConsumer)).named("StreamingTweets"))
      .apply("Extract", ParDo.of(new ExtractEntitiesFn()))
      .apply("Window", Window.<String>into(SlidingWindows.of(Duration.standardSeconds(options.getWindowSize()))
            .every(Duration.standardSeconds(options.getWindowSlide()))
          )
          .triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.ZERO)
          .discardingFiredPanes()
        )
      .apply("Count", Count.<String>perElement())
      .apply("Format", ParDo.of(new FormatAsTextFn()))
      .apply("Output", outputFn)
      ;

    pipeline.run();
  }
}
