package edu.illinois.storm;

import java.util.Map;
import java.util.Random;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * a spout that randomly generate sentence from a set of sentences
 */
public class RandomSentenceSpout extends BaseRichSpout {
  private SpoutOutputCollector _collector;
  private Random _rand;
  private String[] sentences;

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
    sentences =
      new String[]{
        "the cow jumped over the moon",
        "an apple a day keeps the doctor away",
        "four score and seven years ago",
        "snow white and the seven dwarfs",
        "i am at two with nature"
      };
  }

  public void nextTuple() {
    Utils.sleep(200);
    _collector.emit(new Values(sentences[_rand.nextInt(sentences.length)]));
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }
}
