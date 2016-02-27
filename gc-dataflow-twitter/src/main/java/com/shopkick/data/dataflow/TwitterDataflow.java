package com.shopkick.data.dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
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

  static final long WINDOW_SIZE = 600;  // Default window duration in seconds
  static final long WINDOW_SLIDE = 10;  // Default window slide in seconds

  static final String INPUT = "/topics/theneeds0/twstream";  // Default kafka topic to read from
  static final String OUTPUT = "/topics/theneeds0/twout";  // Default kafka topic to read from
  
  /** Extracts entities (hashtags and mentions) from a tweet.
      Input is a json string.
      Supports quoted_status, emitted with the quoting tweet timestamp.
      TODO: use jackson
  */
  static class ExtractEntitiesFn extends DoFn<String, String>{

    //private static final Logger LOG = LoggerFactory.getLogger(ExtractEntitiesFn.class);

    @Override
    public void processElement(ProcessContext c) {

      String in = c.element();
      JSONObject tweet = new JSONObject(in);

      Instant timestamp = new Instant(tweet.getLong("timestamp_ms"));
      //LOG.debug("Timestamp: " + timestamp);

      this.processTweet(c, tweet, timestamp);
      
      if (!tweet.isNull("quoted_status")) {
        //LOG.debug("Found quoted_status");
        JSONObject quotedTweet = tweet.getJSONObject("quoted_status");
        this.processTweet(c, quotedTweet, timestamp);
      }
    }

    protected void processTweet(ProcessContext c, JSONObject tweet, Instant timestamp) {

      JSONObject entities = tweet.getJSONObject("entities");

      JSONArray mentions = entities.getJSONArray("user_mentions");
      for (int i = 0; i < mentions.length(); i++) {
        JSONObject mention = mentions.getJSONObject(i);
        //LOG.debug("Mention: " + "@"+mention.getString("screen_name").toLowerCase());
        //c.outputWithTimestamp("@"+mention.getString("screen_name").toLowerCase(), timestamp);
        c.output("@"+mention.getString("screen_name").toLowerCase());
      }

      JSONArray hashtags = entities.getJSONArray("hashtags");
      for (int i = 0; i < hashtags.length(); i++) {
        JSONObject hashtag = hashtags.getJSONObject(i);
        //LOG.debug("Hashtag: " + "#"+hashtag.getString("text").toLowerCase());
        //c.outputWithTimestamp("#"+hashtag.getString("text").toLowerCase(), timestamp);
        c.output("@"+hashtag.getString("text").toLowerCase());
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
  public static class FormatAsTextFn extends DoFn<KV<String, Long>, String> implements DoFn.RequiresWindowAccess{
    @Override
    public void processElement(ProcessContext c) {
      String text = ((IntervalWindow)c.window()).end() + "," + c.element().getKey() + "," + c.element().getValue();
      c.output(text);
    }
  }

  /**
   * Options supported by {@link TwitterDataflow}.
   * Inherits standard configuration options, and flink-dataflow options.
   */
  public static interface TwitterDataflowOptions extends PipelineOptions, DataflowPipelineOptions {
    @Description("Pubsub topic to read from")
    @Default.String(INPUT)
    String getInput();
    void setInput(String value);

    @Description("Pubsub topic to write to")
    @Default.String(OUTPUT)
    String getOutput();
    void setOutput(String value);

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

    // Cloud Dataflow specific options
    options.setJobName("TwitterDataflow");
    options.setStreaming(true);
    options.setMaxNumWorkers(4);
    
    Pipeline pipeline = Pipeline.create(options);

    pipeline
      .apply("Input", PubsubIO.Read.topic(options.getInput()).timestampLabel("timestamp_ms"))
      .apply("Extract", ParDo.of(new ExtractEntitiesFn()))
      .apply("Window", Window.<String>into(SlidingWindows.of(Duration.standardSeconds(options.getWindowSize()))
            .every(Duration.standardSeconds(options.getWindowSlide()))
          )
          .triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.ZERO)
          .discardingFiredPanes()
        )
      .apply("Count", Count.<String>perElement())
      .apply("Format", ParDo.of(new FormatAsTextFn()))
      .apply("Output", PubsubIO.Write.topic(options.getOutput()))
      ;

    pipeline.run();
  }
}
