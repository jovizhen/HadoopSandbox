package com.weather.hadoop.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.weather.hadoop.util.WeatherDataParser;

public class MaxTempMapper extends Mapper<Text, List<Text>, Text, IntWritable>{

	@Override
	protected void map(Text key, List<Text> value, Context context)
			throws IOException, InterruptedException {
		
		WeatherDataParser parser = new WeatherDataParser();
		for(Text text : value){
			parser.parse(key.toString(), text.toString());
			if(parser.isValidTemperature()){
				context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
			}
		}
	}
}
