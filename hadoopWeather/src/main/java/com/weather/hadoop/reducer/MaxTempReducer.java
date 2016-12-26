package com.weather.hadoop.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable >{

	@Override
	protected void reduce(Text inKey, Iterable<IntWritable> inValue, Context context) throws IOException, InterruptedException {
		int max = Integer.MIN_VALUE;
		for(IntWritable value : inValue){
			if(value.get()>max){
				max = value.get();
			}
		}
		context.write(inKey, new IntWritable(max));
	}
}
