package com.weather.hadoop.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DataConverterMapper extends Mapper<Text, List<Text>, Text, Text>{
	@Override
	protected void map(Text key, List<Text> value, Context context)
			throws IOException, InterruptedException {
		
		for(Text txt: value){
			context.write(key, txt);
		}
	}
}
