package com.weather.hadoop;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.example.hadoop.WordCount;
import com.weather.hadoop.mapper.MaxTempMapper;
import com.weather.hadoop.reducer.MaxTempReducer;
import com.weather.hadoop.util.MultiLineInputFormat;

public class MaxTemperatureDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if(args.length!=2){
			System.err.printf("Usage: %s [generic options] <input> <output\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
		}
		
		Job job = Job.getInstance(getConf(), "Max Temp");
		job.setJarByClass(MaxTemperatureDriver.class);
	    job.setInputFormatClass(MultiLineInputFormat.class);
		
		MultiLineInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(MaxTempMapper.class);
	    job.setCombinerClass(MaxTempReducer.class);
	    job.setReducerClass(MaxTempReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
		System.exit(exitCode);
	}
}
