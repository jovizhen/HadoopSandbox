//cc CutLoadFunc A LoadFunc UDF to load tuple fields as column ranges
package com.jovi.pig;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.jovi.pig.util.MultiLineInputFormat;
import com.jovi.pig.util.MultiLineRecordReader;

// vv CutLoadFunc
public class CutLoadFunc extends LoadFunc {

  private static final Log LOG = LogFactory.getLog(CutLoadFunc.class);

  private final List<Range> hRanges;
  private final List<Range> ranges;
  private final TupleFactory tupleFactory = TupleFactory.getInstance();
  private MultiLineRecordReader reader;

  public CutLoadFunc(String cutHPatternString, String cutPattern) {
    ranges = Range.parse(cutPattern);
    hRanges = Range.parse(cutHPatternString);
  }
  
  @Override
  public void setLocation(String location, Job job)
      throws IOException {
    FileInputFormat.setInputPaths(job, location);
  }
  
  @Override
  public InputFormat getInputFormat() {
    return new MultiLineInputFormat();
  }
  
  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) {
    this.reader = (MultiLineRecordReader) reader;
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      if (!reader.nextKeyValue()) {
        return null;
      }
      Text key = (Text) reader.getCurrentKey();
      List<Text> value = reader.getCurrentValue();
      String line = value.toString();
      int tupleSize = hRanges.size()+ranges.size();
      for (Text rec : value){
    	  Tuple tuple = tupleFactory.newTuple(tupleSize);
      }
      Tuple tuple = tupleFactory.newTuple(tupleSize);
      for (int i = 0; i < ranges.size(); i++) {
        Range range = ranges.get(i);
        if (range.getEnd() > line.length()) {
          LOG.warn(String.format(
              "Range end (%s) is longer than line length (%s)",
              range.getEnd(), line.length()));
          continue;
        }
        tuple.set(i, new DataByteArray(range.getSubstring(line)));
      }
      return tuple;
    } catch (InterruptedException e) {
      throw new ExecException(e);
    }
  }
}
// ^^ CutLoadFunc
