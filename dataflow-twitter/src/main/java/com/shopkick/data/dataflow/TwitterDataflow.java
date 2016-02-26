package com.shopkick.data.dataflow;

import com.dataartisans.flink.dataflow.FlinkPipelineRunner;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.io.UnboundedFlinkSource;
import com.dataartisans.flink.dataflow.FlinkPipelineOptions;
import com.dataartisans.flink.dataflow.io.JdbcIO;

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
 * An example that counts words in Shakespeare and includes Dataflow best practices.
 *
 * <p>This class, {@link WordCount}, is the second in a series of four successively more detailed
 * 'word count' examples. You may first want to take a look at {@link MinimalWordCount}.
 * After you've looked at this example, then see the {@link DebuggingWordCount}
 * pipeline, for introduction of additional concepts.
 *
 * <p>For a detailed walkthrough of this example, see
 *   <a href="https://cloud.google.com/dataflow/java-sdk/wordcount-example">
 *   https://cloud.google.com/dataflow/java-sdk/wordcount-example
 *   </a>
 *
 * <p>Basic concepts, also in the MinimalWordCount example:
 * Reading text files; counting a PCollection; writing to GCS.
 *
 * <p>New Concepts:
 * <pre>
 *   1. Executing a Pipeline both locally and using the Dataflow service
 *   2. Using ParDo with static DoFns defined out-of-line
 *   3. Building a composite transform
 *   4. Defining your own pipeline options
 * </pre>
 *
 * <p>Concept #1: you can execute this pipeline either locally or using the Dataflow service.
 * These are now command-line options and not hard-coded as they were in the MinimalWordCount
 * example.
 * To execute this pipeline locally, specify general pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 * }
 * </pre>
 * and a local output file or output prefix on GCS:
 * <pre>{@code
 *   --output=[YOUR_LOCAL_FILE | gs://YOUR_OUTPUT_PREFIX]
 * }</pre>
 *
 * <p>To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 * }
 * </pre>
 * and an output prefix on GCS:
 * <pre>{@code
 *   --output=gs://YOUR_OUTPUT_PREFIX
 * }</pre>
 *
 * <p>The input file defaults to {@code gs://dataflow-samples/shakespeare/kinglear.txt} and can be
 * overridden with {@code --inputFile}.
 */
public class TwitterDataflow {

  static final String KAFKA_TOPIC = "tweets";  // Default kafka topic to read from
  static final String KAFKA_BROKER = "192.168.99.100:9092";  // Default kafka broker to contact
  static final String GROUP_ID = "myGroup";  // Default groupId
  static final String ZOOKEEPER = "192.168.99.100:2181";  // Default zookeeper to connect to for Kafka
  static final long WINDOW_SIZE = 10;  // Default window duration in seconds
  static final long SLIDE_SIZE = 5;  // Default window slide in seconds

  /**
   * Concept #2: You can make your pipeline code less verbose by defining your DoFns statically out-
   * of-line. This DoFn tokenizes lines of text into individual words; we pass it to a ParDo in the
   * pipeline.
   */
  // static class ExtractWordsFn extends DoFn<String, String> {
  //   private final Aggregator<Long, Long> emptyLines =
  //       createAggregator("emptyLines", new Sum.SumLongFn());

  //   @Override
  //   public void processElement(ProcessContext c) {
  //     if (c.element().trim().isEmpty()) {
  //       emptyLines.addValue(1L);
  //     }

  //     // Split the line into words.
  //     String[] words = c.element().split("[^a-zA-Z']+");

  //     // Output each word encountered into the output PCollection.
  //     for (String word : words) {
  //       if (!word.isEmpty()) {
  //         c.output(word);
  //       }
  //     }
  //   }
  // }

  static class ExtractEntitiesFn extends DoFn<String, String>{

    private static final Logger LOG = LoggerFactory.getLogger(ExtractEntitiesFn.class);

    @Override
    public void processElement(ProcessContext c) {
      // LOG.info("Input: " + c.element());

      String in = c.element();
      JSONObject tweet = new JSONObject(in);

      Instant timestamp = new Instant(tweet.getLong("timestamp_ms"));
      // LOG.info("Timestamp: " + timestamp);

      this.processTweet(c, tweet, timestamp);
      
      if (!tweet.isNull("quoted_status")) {
        // LOG.info("Found quoted_status");
        JSONObject quotedTweet = tweet.getJSONObject("quoted_status");
        this.processTweet(c, quotedTweet, timestamp);
      }
    }

    protected void processTweet(ProcessContext c, JSONObject tweet, Instant timestamp) {

      JSONObject entities = tweet.getJSONObject("entities");

      JSONArray mentions = entities.getJSONArray("user_mentions");
      for (int i = 0; i < mentions.length(); i++) {
        JSONObject mention = mentions.getJSONObject(i);
        // LOG.info("Mention: " + "@"+mention.getString("screen_name").toLowerCase());
        c.outputWithTimestamp("@"+mention.getString("screen_name").toLowerCase(), timestamp);
        // c.output("@"+mention.getString("screen_name").toLowerCase());
      }

      JSONArray hashtags = entities.getJSONArray("hashtags");
      for (int i = 0; i < hashtags.length(); i++) {
        JSONObject hashtag = hashtags.getJSONObject(i);
        // LOG.info("Hashtag: " + "#"+hashtag.getString("text").toLowerCase());
        c.outputWithTimestamp("#"+hashtag.getString("text").toLowerCase(), timestamp);
        // c.output("#"+hashtag.getString("text").toLowerCase());
      }      
    }

    @Override
    public Duration getAllowedTimestampSkew(){
      return Duration.millis(Long.MAX_VALUE);
    }
  }


  /** A SimpleFunction that converts a Word and Count into a printable string. */
  // static class TransformForPrint extends DoFn<KV<String,List<KV<String,Long>>>, String>
  public static class FormatAsTextFn extends DoFn<KV<String, Long>, String> {
  // public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
    @Override
    public void processElement(ProcessContext c) {
    // public String apply(KV<String, Long> input) {
      String text = ((IntervalWindow)c.window()).start() + "," + c.element().getKey() + "," + c.element().getValue();
      c.output(text);
    }
  }

  /**
   * A PTransform that converts a PCollection containing lines of text into a PCollection of
   * formatted word counts.
   *
   * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
   * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
   * modular testing, and an improved monitoring experience.
   */
  // public static class CountWords extends PTransform<PCollection<String>,
  //     PCollection<KV<String, Long>>> {
  //   @Override
  //   public PCollection<KV<String, Long>> apply(PCollection<String> lines) {

  //     // Convert lines of text into individual words.
  //     PCollection<String> words = lines.apply(
  //         ParDo.of(new ExtractWordsFn()));

  //     // Count the number of times each word occurs.
  //     PCollection<KV<String, Long>> wordCounts =
  //         words.apply(Count.<String>perElement());

  //     return wordCounts;
  //   }
  // }

  public static class CountEntities extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> apply(PCollection<String> tweets) {

      // Convert tweets (one per line) into individual entities.
      PCollection<String> entities = tweets
        .apply(ParDo.of(new ExtractEntitiesFn()));

      // Count the number of times each entity occurs.
      PCollection<KV<String, Long>> entityCounts = entities
        .apply(Count.<String>perElement());

      return entityCounts;
    }
  }

  /**
   * Options supported by {@link WordCount}.
   *
   * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments
   * to be processed by the command-line parser, and specify default values for them. You can then
   * access the options values in your pipeline code.
   *
   * <p>Inherits standard configuration options.
   */
  public static interface TwitterDataflowOptions extends PipelineOptions, FlinkPipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
    String getInput();
    void setInput(String value);

    @Description("Path of the file to write to")
    @Default.InstanceFactory(OutputFactory.class)
    String getOutput();
    void setOutput(String value);

    @Description("The Kafka topic to read from")
    @Default.String(KAFKA_TOPIC)
    String getKafkaTopic();

    void setKafkaTopic(String value);

    @Description("The Kafka Broker to read from")
    @Default.String(KAFKA_BROKER)
    String getBroker();

    void setBroker(String value);

    @Description("The Zookeeper server to connect to")
    @Default.String(ZOOKEEPER)
    String getZookeeper();

    void setZookeeper(String value);

    @Description("The groupId")
    @Default.String(GROUP_ID)
    String getGroup();

    void setGroup(String value);

    @Description("Sliding window duration, in seconds")
    @Default.Long(WINDOW_SIZE)
    Long getWindowSize();

    void setWindowSize(Long value);

    @Description("Window slide, in seconds")
    @Default.Long(SLIDE_SIZE)
    Long getSlide();

    void setSlide(Long value);

    /**
     * Returns "gs://${YOUR_STAGING_DIRECTORY}/counts.txt" as the default destination.
     */
    public static class OutputFactory implements DefaultValueFactory<String> {
      @Override
      public String create(PipelineOptions options) {
        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        if (dataflowOptions.getStagingLocation() != null) {
          return GcsPath.fromUri(dataflowOptions.getStagingLocation())
              .resolve("counts.txt").toString();
        } else {
          throw new IllegalArgumentException("Must specify --output or --stagingLocation");
        }
      }
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(ExtractEntitiesFn.class);

  public static void main(String[] args) {

    Boolean isFlink = true;

    // if (isFlink) {
    //   setJobName
    // }

    PipelineOptionsFactory.register(TwitterDataflowOptions.class);

    TwitterDataflowOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(TwitterDataflowOptions.class);

    options.setJobName("TwitterDataflow");
    options.setStreaming(true);
    options.setRunner(FlinkPipelineRunner.class);

    Pipeline pipeline = Pipeline.create(options);

    Properties props = new Properties();
    props.setProperty("zookeeper.connect", options.getZookeeper());
    props.setProperty("bootstrap.servers", options.getBroker());
    props.setProperty("group.id", options.getGroup());

    // this is the Flink consumer that reads the input to
    // the program from a kafka topic.
    FlinkKafkaConsumer082 kafkaConsumer = new FlinkKafkaConsumer082<String>(
        options.getKafkaTopic(),
        new SimpleStringSchema(), props);

    FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<String>(
        "output",
        new JavaDefaultStringSchema(), props, null);

    JdbcIO.Write.Bound<String> kafkaApply = new JdbcIO.Write.Bound<String>("WriteEntityCounts", kafkaProducer);
    FlinkKafkaProducer x = kafkaApply.getKafkaProducer();
    LOG.info(x.toString());

    // Concepts #2 and #3: Our pipeline applies the composite CountEntities transform, and passes the
    // static FormatAsTextFn() to the ParDo transform.
    // PCollection<String> entities = 
      pipeline
     // .apply(TextIO.Read.named("ReadLines").from(options.getInput()))
     // .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply(Read.named("ReadTweets").from(new UnboundedFlinkSource<String, UnboundedSource.CheckpointMark>(options, kafkaConsumer)).named("StreamingTweets"))
        .apply("Extract", ParDo.of(new ExtractEntitiesFn()))
        .apply("Window", Window.<String>into(SlidingWindows.of(Duration.standardSeconds(options.getWindowSize()))
              .every(Duration.standardSeconds(options.getSlide()))
            )
            .triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes()
          )
        // ;

    // Count the number of times each entity occurs.
    // PCollection<KV<String, Long>> entityCounts = entities
        .apply("Count", Count.<String>perElement())
        // ;

    // entityCounts
        .apply(ParDo.of(new FormatAsTextFn()))
        .apply(kafkaApply);

     // .apply(new CountEntities())

    pipeline.run();
  }
}
