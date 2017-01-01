package com.weather.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class DataConverterReducer extends Reducer<Text, Text, Text, Text >{

	@Override
	protected void reduce(Text inKey, Iterable<Text> inValue, Context context) throws IOException, InterruptedException {
		for(Text value : inValue){
			context.write(inKey, value);
		}
	}
}
