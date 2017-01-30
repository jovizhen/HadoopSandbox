package com.hadoopbook.hbase;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hadoopbook.hbase.mapper.HBaseTemperatureMapper;



public class HBaseTemperatureImporter extends Configured implements Tool {

	private static final String TBL_NAME_OBSERVATION = "observation";
	
	public int run(String[] args) throws Exception {
		if(args.length!=2){
			System.err.printf("Usage: %s [generic options] <input> <output\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
		}
		
		Job job = Job.getInstance(getConf(), "Max Temp");
		job.setJarByClass(getClass());
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TBL_NAME_OBSERVATION);
	    job.setMapperClass(HBaseTemperatureMapper.class);
	    job.setOutputFormatClass(TableOutputFormat.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new HBaseTemperatureImporter(), args);
		System.exit(exitCode);
	}
}
