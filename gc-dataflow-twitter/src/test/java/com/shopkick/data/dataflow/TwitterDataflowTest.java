package com.shopkick.data.dataflow;

import com.shopkick.data.dataflow.TwitterDataflow.ExtractEntitiesFn;
import com.shopkick.data.dataflow.TwitterDataflow.FormatAsTextGlobalFn;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.coders.StringDelegateCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Joiner;

import org.apache.flink.test.util.JavaProgramTestBase;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;
import java.net.URI;

/**
 * Tests of TwitterDataflow.
 */
@RunWith(JUnit4.class)
public class TwitterDataflowTest extends JavaProgramTestBase {

  protected String resultPath;

  static final String[] TWEETS_ARRAY = new String[] {
    "{\"text\": \"RT @TheEllenShow: If only Bradley's arm was longer. Best photo ever. #Oscars2016 http://t.co/C9U5NOtGap\", \"id\": 700378389887262720, \"timestamp_ms\": \"1455818195207\", \"entities\": {\"user_mentions\": [{\"id\": 15846407, \"indices\": [3, 16], \"id_str\": \"15846407\", \"screen_name\": \"TheEllenShow\", \"name\": \"Ellen DeGeneres\"}], \"symbols\": [], \"hashtags\": [{\"indices\": [69, 80], \"text\": \"Oscars2016\"}], \"urls\": []}, \"id_str\": \"440322224407314432\"}",
    "{\"text\": \"Friends of https://t.co/lurLYgpDYi, look what RamonaZacharias is sharing today... https://t.co/i0imILr49N #nuascannan\", \"is_quote_status\": true, \"id\": 700378821766336512, \"timestamp_ms\": \"1455818347302\", \"entities\": {\"user_mentions\": [], \"symbols\": [], \"hashtags\": [{\"indices\": [106, 117], \"text\": \"nuascannan\"}], \"urls\": [{\"url\": \"https://t.co/lurLYgpDYi\", \"indices\": [11, 34], \"expanded_url\": \"http://nuascannan.com\", \"display_url\": \"nuascannan.com\"}, {\"url\": \"https://t.co/i0imILr49N\", \"indices\": [82, 105], \"expanded_url\": \"http://twitter.com/RamonaZacharias/status/700378168465756160\", \"display_url\": \"twitter.com/RamonaZacharia…\"}]}, \"quoted_status_id\": 700366240066498561, \"quoted_status\": {\"text\": \".@CreativeScreen interviewed the writers of ALL 10 #Oscars2016 screenplay nominees https://t.co/idTxQkUqDp #screenwriting #Oscars #scriptchat\", \"id\": 700366240066498561, \"timestamp_ms\": \"1455818195207\", \"entities\": {\"user_mentions\": [{\"id\": 54023853, \"indices\": [1, 16], \"id_str\": \"54023853\", \"screen_name\": \"CreativeScreen\", \"name\": \"Cr. Screenwriting\"}], \"symbols\": [], \"hashtags\": [{\"indices\": [51, 61], \"text\": \"Oscars2016\"}, {\"indices\": [106, 120], \"text\": \"screenwriting\"}, {\"indices\": [121, 128], \"text\": \"Oscars\"}, {\"indices\": [129, 140], \"text\": \"scriptchat\"}], \"urls\": [{\"url\": \"https://t.co/idTxQkUqDp\", \"indices\": [82, 105], \"expanded_url\": \"http://creativescreenwriting.com/screenwriters-at-the-oscars-2016/\", \"display_url\": \"creativescreenwriting.com/screenwriters-…\"}]}, \"id_str\": \"700366240066498561\"}, \"id_str\": \"700378821766336512\"}",
    "{\"text\": \"Throwing together a last minute #Oscars2016 party? Here’s some tips to make it extra fabulous. https://t.co/p5g8fAniwo\", \"id\": 700379027819753472, \"timestamp_ms\": \"1455818347302\", \"entities\": {\"user_mentions\": [], \"symbols\": [], \"hashtags\": [{\"indices\": [32, 43], \"text\": \"Oscars2016\"}], \"urls\": [{\"url\": \"https://t.co/p5g8fAniwo\", \"indices\": [95, 118], \"expanded_url\": \"http://louwhatwear.com/inspiration-events-how-to-throw-the-ultimate-oscar-party/\", \"display_url\": \"louwhatwear.com/inspiration-ev…\"}]}, \"id_str\": \"700379027819753472\"}"
  };

  static final List<String> TWEETS = Arrays.asList(TWEETS_ARRAY);

  static final String[] EXPECTED_RESULT = new String[] {
      "#nuascannan,1", "#oscars2016,3", "@theellenshow,1", "@creativescreen,1", "#screenwriting,1", "#oscars,1", "#scriptchat,1"};


  @Override
  protected void preSubmit() throws Exception {
    resultPath = getTempDirPath("result");
  }

  @Override
  protected void postSubmit() throws Exception {
    compareResultsByLinesInMemory(Joiner.on('\n').join(EXPECTED_RESULT), resultPath);
  }

  @Override
  protected void testProgram() throws Exception {
    Pipeline pipeline = FlinkTestPipeline.create();

    pipeline.getCoderRegistry().registerCoder(URI.class, StringDelegateCoder.of(URI.class));

    pipeline
      .apply("Input", Create.of(TWEETS).withCoder(StringUtf8Coder.of())) // input from String[]
      .apply("Extract", ParDo.of(new ExtractEntitiesFn()))
      // no windowing
      .apply("Count", Count.<String>perElement())
      .apply("Format", ParDo.of(new FormatAsTextGlobalFn())) // format with Global window
      .apply("Output", TextIO.Write.to(resultPath)) // output in temp text
      ;

    pipeline.run();
  }

}