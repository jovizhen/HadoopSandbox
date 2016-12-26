package com.example.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.example.hadoop.mapper.WordCountMapper;
import com.example.hadoop.reducer.WordCountIntSumReducer;

public class WordCount extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if(args.length!=2){
			System.err.printf("Usage: %s [generic options] <input> <output\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
		}
		
		Job job = Job.getInstance(getConf(), "word count");
	    job.setJarByClass(WordCount.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(WordCountMapper.class);
	    job.setCombinerClass(WordCountIntSumReducer.class);
	    job.setReducerClass(WordCountIntSumReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);
	}
}
