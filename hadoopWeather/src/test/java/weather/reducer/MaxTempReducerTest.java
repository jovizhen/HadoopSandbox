package weather.reducer;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.weather.hadoop.reducer.MaxTempReducer;

public class MaxTempReducerTest {
	
	@Test
	public void testMaxTempReducer() throws IOException{
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		.withReducer(new MaxTempReducer())
		.withInput(new Text("1980"), Arrays.asList(new IntWritable(69), new IntWritable(62),new IntWritable(89)))
		.withOutput(new Text("1980"), new IntWritable(89)).runTest();
	}
}
