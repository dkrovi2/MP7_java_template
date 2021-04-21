package edu.illinois.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * a bolt that tracks word count
 */
public class WordCountBolt extends BaseBasicBolt {
  // Hint: Add necessary instance variables if needed
  private final Map<String, Integer> countByWord = new HashMap<>();

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    /* ----------------------TODO-----------------------
    Task: word count
		Hint: using instance variable to tracking the word count
    ------------------------------------------------- */

    // End
    String word = tuple.getString(0);
    int count = countByWord.getOrDefault(word, 0);
    count++;
    countByWord.put(word, count);
    collector.emit(new Values(word, count));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count"));
  }
}
