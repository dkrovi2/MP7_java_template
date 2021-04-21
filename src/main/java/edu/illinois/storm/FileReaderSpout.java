package edu.illinois.storm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Map;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * a spout that generate sentences from a file
 */
public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext _context;
  private String inputFile;

  // Hint: Add necessary instance variables if needed
  private LineNumberReader reader;

  // Set input file path
  public FileReaderSpout withInputFileProperties(String inputFile) {
    this.inputFile = inputFile;
    return this;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this._context = context;
    this._collector = collector;

    try {
      reader = new LineNumberReader(new FileReader(inputFile));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void nextTuple() {
    if (reader == null) {
      return;
    }
    try {
      String line = reader.readLine();
      if (null != line) {
        _collector.emit(new Values(line));
      } else {
        Utils.sleep(1000);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

  @Override
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void fail(Object msgId) {
  }

  public void ack(Object msgId) {
  }

  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
