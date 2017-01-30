package com.hadoopbook.hbase.mapper;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoopbook.hbase.NcdcDataParser;

public class HBaseTemperatureMapper<K> extends Mapper<LongWritable, Text, K, Put>{
	
	private static final byte[] COLUMN_FAMILY = Bytes.toBytes("info");
	private static final byte[] AIRTEMP_QUALIFIER = Bytes.toBytes("temperature");

	private NcdcDataParser parser = new NcdcDataParser();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, K, Put>.Context context)
			throws IOException, InterruptedException {

		parser.parse(value.toString(), "");
		
		if(parser.isValidTemperature()){
			byte[] rowKey = parser.getYear().getBytes();
			Put p = new Put(rowKey);
			p.addColumn(COLUMN_FAMILY, AIRTEMP_QUALIFIER, Bytes.toBytes(parser.getAirTemperature()));
			context.write(null, p);
		}
	}
}
