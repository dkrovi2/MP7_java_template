package edu.illinois.storm;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseRichBolt {
  private OutputCollector collector;
  private int topN;

  public PriorityQueue<Entry> sortByCountDesc;

  public static class Entry {
    public static final Comparator<Entry> SORT_BY_COUNT_DESC =
      Comparator.comparing(Entry::getCount).reversed();
    String word;
    int count;

    public Entry(String w, int c) {
      this.word = w;
      this.count = c;
    }

    public String getWord() {
      return word;
    }

    public int getCount() {
      return count;
    }

    @Override
    public String toString() {
      return "Entry{" +
        "word='" + word + '\'' +
        ", count=" + count +
        '}';
    }
  }

  // Hint: Add necessary instance variables and inner classes if needed


  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    this.sortByCountDesc = new PriorityQueue<>(Entry.SORT_BY_COUNT_DESC);
  }

  public TopNFinderBolt withNProperties(int N) {
    this.topN = N;
    return this;
  }

  @Override
  public void execute(Tuple tuple) {
    String word = tuple.getString(0);
    int count = tuple.getInteger(1);
    sortByCountDesc.offer(new Entry(word, count));
    List<Entry> topN = new ArrayList<>();
    int ctr = 0;
    while (!sortByCountDesc.isEmpty() && ctr < this.topN) {
      ctr++;
      topN.add(sortByCountDesc.poll());
    }
    System.out.println(topN);
    collector.emit(new Values(
      StringUtils.join(
        topN.stream().map(s -> s.word).collect(Collectors.toList()), ", ")));
    sortByCountDesc.addAll(topN);
    /* ----------------------TODO-----------------------
    Task: keep track of the top N words
		Hint: implement efficient algorithm so that it won't be shutdown before task finished
		      the algorithm we used when we developed the auto-grader is maintaining a N size min-heap
    ------------------------------------------------- */

    // End
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("top-N"));

  }

}
