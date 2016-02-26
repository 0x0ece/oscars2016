package com.shopkick.data.dataflow;

import com.shopkick.data.dataflow.TwitterDataflowSpark.CountEntities;
import com.shopkick.data.dataflow.TwitterDataflowSpark.ExtractEntitiesFn;
import com.shopkick.data.dataflow.TwitterDataflowSpark.FormatAsTextFn;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests of TwitterDataflow.
 */
@RunWith(JUnit4.class)
public class TwitterDataflowTest {

  /** Example test that tests a specific DoFn. */
  @Test
  public void testExtractEntitiesFn() {
    DoFnTester<String, String> extractFn =
        DoFnTester.of(new ExtractEntitiesFn());

    Assert.assertThat(extractFn.processBatch("{\"text\": \"RT @TheEllenShow: If only Bradley's arm was longer. Best photo ever. #Oscars2016 http://t.co/C9U5NOtGap\", \"id\": 700378389887262720, \"timestamp_ms\": \"1455818195207\", \"entities\": {\"user_mentions\": [{\"id\": 15846407, \"indices\": [3, 16], \"id_str\": \"15846407\", \"screen_name\": \"TheEllenShow\", \"name\": \"Ellen DeGeneres\"}], \"symbols\": [], \"hashtags\": [{\"indices\": [69, 80], \"text\": \"Oscars2016\"}], \"urls\": []}, \"id_str\": \"440322224407314432\"}"),
                      CoreMatchers.hasItems("@theellenshow", "#oscars2016"));
    // Assert.assertThat(extractFn.processBatch(" "),
    //                   CoreMatchers.<String>hasItems());
    // Assert.assertThat(extractFn.processBatch(" some ", " input", " words"),
    //                   CoreMatchers.hasItems("some", "input", "words"));
  }

  static final String[] WORDS_ARRAY = new String[] {
    "{\"text\": \"RT @TheEllenShow: If only Bradley's arm was longer. Best photo ever. #Oscars2016 http://t.co/C9U5NOtGap\", \"id\": 700378389887262720, \"timestamp_ms\": \"1455818195207\", \"entities\": {\"user_mentions\": [{\"id\": 15846407, \"indices\": [3, 16], \"id_str\": \"15846407\", \"screen_name\": \"TheEllenShow\", \"name\": \"Ellen DeGeneres\"}], \"symbols\": [], \"hashtags\": [{\"indices\": [69, 80], \"text\": \"Oscars2016\"}], \"urls\": []}, \"id_str\": \"440322224407314432\"}",
    "{\"text\": \"Friends of https://t.co/lurLYgpDYi, look what RamonaZacharias is sharing today... https://t.co/i0imILr49N #nuascannan\", \"is_quote_status\": true, \"id\": 700378821766336512, \"timestamp_ms\": \"1455818347302\", \"entities\": {\"user_mentions\": [], \"symbols\": [], \"hashtags\": [{\"indices\": [106, 117], \"text\": \"nuascannan\"}], \"urls\": [{\"url\": \"https://t.co/lurLYgpDYi\", \"indices\": [11, 34], \"expanded_url\": \"http://nuascannan.com\", \"display_url\": \"nuascannan.com\"}, {\"url\": \"https://t.co/i0imILr49N\", \"indices\": [82, 105], \"expanded_url\": \"http://twitter.com/RamonaZacharias/status/700378168465756160\", \"display_url\": \"twitter.com/RamonaZacharia…\"}]}, \"quoted_status_id\": 700366240066498561, \"quoted_status\": {\"text\": \".@CreativeScreen interviewed the writers of ALL 10 #Oscars2016 screenplay nominees https://t.co/idTxQkUqDp #screenwriting #Oscars #scriptchat\", \"id\": 700366240066498561, \"timestamp_ms\": \"1455818195207\", \"entities\": {\"user_mentions\": [{\"id\": 54023853, \"indices\": [1, 16], \"id_str\": \"54023853\", \"screen_name\": \"CreativeScreen\", \"name\": \"Cr. Screenwriting\"}], \"symbols\": [], \"hashtags\": [{\"indices\": [51, 61], \"text\": \"Oscars2016\"}, {\"indices\": [106, 120], \"text\": \"screenwriting\"}, {\"indices\": [121, 128], \"text\": \"Oscars\"}, {\"indices\": [129, 140], \"text\": \"scriptchat\"}], \"urls\": [{\"url\": \"https://t.co/idTxQkUqDp\", \"indices\": [82, 105], \"expanded_url\": \"http://creativescreenwriting.com/screenwriters-at-the-oscars-2016/\", \"display_url\": \"creativescreenwriting.com/screenwriters-…\"}]}, \"id_str\": \"700366240066498561\"}, \"id_str\": \"700378821766336512\"}",
    "{\"text\": \"Throwing together a last minute #Oscars2016 party? Here’s some tips to make it extra fabulous. https://t.co/p5g8fAniwo\", \"id\": 700379027819753472, \"timestamp_ms\": \"1455818347302\", \"entities\": {\"user_mentions\": [], \"symbols\": [], \"hashtags\": [{\"indices\": [32, 43], \"text\": \"Oscars2016\"}], \"urls\": [{\"url\": \"https://t.co/p5g8fAniwo\", \"indices\": [95, 118], \"expanded_url\": \"http://louwhatwear.com/inspiration-events-how-to-throw-the-ultimate-oscar-party/\", \"display_url\": \"louwhatwear.com/inspiration-ev…\"}]}, \"id_str\": \"700379027819753472\"}"
  };

  static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

  static final String[] COUNTS_ARRAY = new String[] {
      "#nuascannan: 1", "#oscars2016: 3", "@theellenshow: 1", "@creativescreen: 1", "#screenwriting: 1", "#oscars: 1", "#scriptchat: 1"};

  /** Example test that tests a PTransform by using an in-memory input and inspecting the output. */
  @Test
  @Category(RunnableOnService.class)
  public void testTwitterDataflow() throws Exception {
    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()));

    PCollection<String> output = input.apply(new CountEntities())
      .apply(ParDo.of(new FormatAsTextFn()));

    DataflowAssert.that(output).containsInAnyOrder(COUNTS_ARRAY);
    p.run();
  }
}