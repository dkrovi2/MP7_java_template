package edu.illinois.storm;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * A bolt that normalizes the words, by removing common words and making them lower case.
 */
public class NormalizerBolt extends BaseBasicBolt {
  private static final List<String> commonWords =
    Arrays.asList(
      "the", "be", "a", "an", "and", "of", "to", "in", "am", "is", "are", "at", "not", "that",
      "have", "i", "it", "for", "on", "with", "he", "she", "as", "you", "do", "this", "but",
      "his", "by", "from", "they", "we", "her", "or", "will", "my", "one", "all", "s", "if",
      "any", "our", "may", "your", "these", "d", " ", "me", "so", "what", "him", "their");

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String word = tuple.getString(0).toLowerCase(Locale.ROOT);
    if (!commonWords.contains(word)) {
      collector.emit(new Values(word));
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }
}
