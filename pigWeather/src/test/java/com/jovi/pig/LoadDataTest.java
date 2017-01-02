package com.jovi.pig;


import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.junit.Before;
import org.junit.Test;

import com.jovi.pig.util.WeatherRangeSpecIGRA2;

public class LoadDataTest {
	 private CutLoadFunc func;
	    
	@Before
	public void setUp() throws IOException, InterruptedException{
		func = new CutLoadFunc("year, temp, lat, lon");
		JobConf conf = createConfig();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        TextInputFormat inputFormat = (TextInputFormat) func.getInputFormat();
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()){
        });
        RecordReader<LongWritable, Text> recordReader = inputFormat.createRecordReader(actualSplits.get(0), context);
        recordReader.initialize(actualSplits.get(0), context);
        PigSplit pigSplit = new PigSplit(new InputSplit[] {actualSplits.get(0)}, -1,
                new ArrayList<OperatorKey>(), -1);
        pigSplit.setConf(conf);
        func.prepareToRead(recordReader, pigSplit);
	}
	
	@Test
	public void testEnum() throws IOException {
		assertEquals(WeatherRangeSpecIGRA2.YEAR.spec(), WeatherRangeSpecIGRA2.valueOf("year".toUpperCase()).spec());
		assertEquals(WeatherRangeSpecIGRA2.LON.spec(), WeatherRangeSpecIGRA2.valueOf("lon".toUpperCase()).spec());
		assertEquals(WeatherRangeSpecIGRA2.LAT.spec(), WeatherRangeSpecIGRA2.valueOf("lat".toUpperCase()).spec());
		assertEquals(WeatherRangeSpecIGRA2.TEMP.spec(), WeatherRangeSpecIGRA2.valueOf("temp".toUpperCase()).spec());
	}
	
	@Test
	public void testCustLoadfFunc() throws IOException, InterruptedException{
        
        Tuple tuple =func.getNext();
        assertEquals("1947", tuple.get(0).toString());
        assertEquals("145", tuple.get(1).toString());
        assertEquals("171170", tuple.get(2).toString());
        assertEquals("-617830", tuple.get(3).toString());
        
	}
	
	private JobConf createConfig() {
        JobConf conf = new JobConf();
        System.out.println("start testing .txt files");
        conf.setStrings("mapreduce.input.fileinputformat.inputdir", "./fixtures/test.txt");
        return conf;
    }
}
