package com.weather.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.weather.hadoop.mapper.DataConverterMapper;
import com.weather.hadoop.reducer.DataConverterReducer;
import com.weather.hadoop.util.MultiLineInputFormat;

public class DataConverterDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if(args.length!=2){
			System.err.printf("Usage: %s [generic options] <input> <output\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
		}
		
		Job job = Job.getInstance(getConf(), "Data Converter");
		job.setJarByClass(DataConverterDriver.class);
		job.setInputFormatClass(MultiLineInputFormat.class);
		
		job.setReducerClass(DataConverterReducer.class);
		job.setMapperClass(DataConverterMapper.class);
		
		MultiLineInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(2);
		return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new DataConverterDriver(), args);
		System.exit(exitCode);
	}

}
