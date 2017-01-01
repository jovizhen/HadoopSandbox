package weather.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import com.weather.hadoop.mapper.MaxTempMapper;
import com.weather.hadoop.util.ArrayListTextWritable;

public class MaxTemMapperTest {

	@Test
	public void processValidRecord() throws IOException{
		Text key = new Text("#AGM00060559 1973 01 01 18 9999    7 usaf-ds3 usaf-ds3  335000    67830");
		Text val1 = new Text("30 -9999  -9999  2000    50 -9999 -9999   280    77");
		Text val2 = new Text("10 -9999  85000 -9999 -9999 -9999 -9999   320    77");
		List<Text> value = new ArrayListTextWritable();
		value.add(val1);
		value.add(val2);
		Mapper<Text, List<Text>, Text, IntWritable> tempMapper = new MaxTempMapper();
		
		new MapDriver<Text, List<Text>, Text, IntWritable>()
		.withMapper(tempMapper)
		.withInput(key, value).withOutput(new Text("1973"), new IntWritable(50))
		.runTest();
	}
}
