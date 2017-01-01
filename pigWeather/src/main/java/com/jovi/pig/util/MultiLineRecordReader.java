package com.jovi.pig.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MultiLineRecordReader extends RecordReader<Text, List<Text>>{

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	protected Reader in;
	private Text key = null;
	private List<Text> value = null;
	private Boolean isZipFile = false;
	private InputStream is;
	
	
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration conf = context.getConfiguration();

		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		isZipFile = file.getName().endsWith(".zip");
		compressionCodecs = new CompressionCodecFactory(conf);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(conf);
		FSDataInputStream fileIn = fs.open(split.getPath());

		if (codec != null) {
			is = codec.createInputStream(fileIn);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				fileIn.seek(start);
			}
			is = fileIn;
		}

		this.pos = start;
		init(is, conf);
	}
	
	/**
	 * reads configuration set in the runner, setting delimiter and separator to
	 * be used to process the CSV file . If isZipFile is set, creates a
	 * ZipInputStream on top of the InputStream
	 * 
	 * @param is
	 *            - the input stream
	 * @param conf
	 *            - hadoop conf
	 * @throws IOException
	 */
	public void init(InputStream is, Configuration conf) throws IOException {
		if (isZipFile) {
			ZipInputStream zis = new ZipInputStream(new BufferedInputStream(is));
			zis.getNextEntry();
			is = zis;
		}
		this.is = is;
		this.in = new BufferedReader(new InputStreamReader(is));
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(key == null){
			key = new Text();
		}
		
		if (value == null) {
			value = new ArrayListTextWritable();
		}
		while (true) {
			if (pos >= end)
				return false;
			int newSize = 0;
			newSize = readLine(key, value);
			pos += newSize;
			if (newSize == 0) {
//				if (isZipFile) {
//					ZipInputStream zis = (ZipInputStream) is;
//					if (zis.getNextEntry() != null) {
//						is = zis;
//						in = new BufferedReader(new InputStreamReader(is));
//						continue;
//					}
//				}
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}
	}

	
	/**
	 * Parses a line from the CSV, from the current stream position. It stops
	 * parsing when it finds a new line char outside two delimiters
	 * 
	 * @param values
	 *            List of column values parsed from the current CSV line
	 * @return number of chars processed from the stream
	 * @throws IOException
	 */
	protected int readLine(Text key, List<Text> values) throws IOException {
		key.clear();
		values.clear();
		char c;
		int numRead = 0;
		StringBuffer sb = new StringBuffer();
		int i;
		int numOfRecords=0;
		
		while ((i = in.read()) != -1) {
			c = (char) i;
			numRead++;
			
			if (c == '\n'){
				String line = sb.toString().trim();
				if(line.startsWith("#")){
					key.set(line);
					numOfRecords=Integer.parseInt(line.substring(33, 37).trim());
				}else{
					numOfRecords--;
					value.add(new Text(line));
					if(numOfRecords==0)
						break;
				}
				sb.delete(0, sb.length());
			}else{
				sb.append(c);
			}
		}
		return numRead;
	}
	
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		
		return key;
	}

	@Override
	public List<Text> getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	@Override
	public void close() throws IOException {
		if (in != null) {
			in.close();
			in = null;
		}
		if (is != null) {
			is.close();
			is = null;
		}
	}
}
