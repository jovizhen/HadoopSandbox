package weather.input;

import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import com.weather.hadoop.util.MultiLineInputFormat;

public class MultipleLineInputFormatTest {
	
	@Test
	public void shouldReturnListsAsRecords() throws Exception {
		JobConf conf = createConfig();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        MultiLineInputFormat inputFormat = new MultiLineInputFormat();
        
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()){

        });
        
        RecordReader<Text, List<Text>> recordReader = inputFormat.createRecordReader(actualSplits.get(0), context);
        recordReader.initialize(actualSplits.get(0), context);

        recordReader.nextKeyValue();
        List<Text> firstLineValue = recordReader.getCurrentValue();
        Text firstKey = recordReader.getCurrentKey();
        
        System.out.println("first key: " + firstKey.toString());
        System.out.println("first line rec#0: " + firstLineValue.get(0).toString());
        System.out.println("first line rec#1: " + firstLineValue.get(1).toString());
        
        assertEquals("#AGM00060559 1973 01 01 18 9999    7 usaf-ds3 usaf-ds3  335000    67830", firstKey.toString());
        assertEquals("10 -9999  85000 -9999 -9999 -9999 -9999   320    77", firstLineValue.get(0).toString());
        assertEquals("10 -9999  70000 -9999 -9999 -9999 -9999   320    87", firstLineValue.get(1).toString());
        
        recordReader.nextKeyValue();
        List<Text> secLineValue = recordReader.getCurrentValue();
        Text secKey = recordReader.getCurrentKey();
        
        System.out.println("sec key: " + secKey.toString());
        System.out.println("sec line rec#0: " + secLineValue.get(0).toString());
        System.out.println("sec line rec#1: " + secLineValue.get(1).toString());
        
        assertEquals("#AGM00060559 1973 01 02 12 9999   15 usaf-ds3 usaf-ds3  335000    67830", secKey.toString());
        assertEquals("10 -9999  85000 -9999 -9999 -9999 -9999   310    77", secLineValue.get(0).toString());
        assertEquals("10 -9999  70000 -9999 -9999 -9999 -9999   320    67", secLineValue.get(1).toString());
        
        recordReader.nextKeyValue();
        List<Text> thrLineValue = recordReader.getCurrentValue();
        Text thrKey = recordReader.getCurrentKey();
        
        System.out.println("thr key: " + thrKey.toString());
        System.out.println("thr line rec#0: " + thrLineValue.get(0).toString());
        System.out.println("thr line rec#1: " + thrLineValue.get(1).toString());
        
        assertEquals("#AGM00060559 1973 01 02 18 9999   13 usaf-ds3 usaf-ds3  335000    67830", thrKey.toString());
        assertEquals("10 -9999  85000 -9999 -9999 -9999 -9999   320    61", thrLineValue.get(0).toString());
        assertEquals("10 -9999  70000 -9999 -9999 -9999 -9999   330    92", thrLineValue.get(1).toString());
	}
	
	@Test
	public void shouldReturnListsAsRecords_zip() throws Exception {
		JobConf conf = creatConfigZip();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        MultiLineInputFormat inputFormat = new MultiLineInputFormat();
        
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()){

        });
        
        RecordReader<Text, List<Text>> recordReader = inputFormat.createRecordReader(actualSplits.get(0), context);
        recordReader.initialize(actualSplits.get(0), context);

        recordReader.nextKeyValue();
        List<Text> firstLineValue = recordReader.getCurrentValue();
        Text firstKey = recordReader.getCurrentKey();
        
        System.out.println("first key: " + firstKey.toString());
        System.out.println("first line rec#0: " + firstLineValue.get(0).toString());
        System.out.println("first line rec#1: " + firstLineValue.get(1).toString());
        
        assertEquals("#AGM00060559 1973 01 01 18 9999    7 usaf-ds3 usaf-ds3  335000    67830", firstKey.toString());
        assertEquals("10 -9999  85000 -9999 -9999 -9999 -9999   320    77", firstLineValue.get(0).toString());
        assertEquals("10 -9999  70000 -9999 -9999 -9999 -9999   320    87", firstLineValue.get(1).toString());
        
        recordReader.nextKeyValue();
        List<Text> secLineValue = recordReader.getCurrentValue();
        Text secKey = recordReader.getCurrentKey();
        
        System.out.println("sec key: " + secKey.toString());
        System.out.println("sec line rec#0: " + secLineValue.get(0).toString());
        System.out.println("sec line rec#1: " + secLineValue.get(1).toString());
        
        assertEquals("#AGM00060559 1973 01 02 12 9999   15 usaf-ds3 usaf-ds3  335000    67830", secKey.toString());
        assertEquals("10 -9999  85000 -9999 -9999 -9999 -9999   310    77", secLineValue.get(0).toString());
        assertEquals("10 -9999  70000 -9999 -9999 -9999 -9999   320    67", secLineValue.get(1).toString());
        
        recordReader.nextKeyValue();
        List<Text> thrLineValue = recordReader.getCurrentValue();
        Text thrKey = recordReader.getCurrentKey();
        
        System.out.println("thr key: " + thrKey.toString());
        System.out.println("thr line rec#0: " + thrLineValue.get(0).toString());
        System.out.println("thr line rec#1: " + thrLineValue.get(1).toString());
        
        assertEquals("#AGM00060559 1973 01 02 18 9999   13 usaf-ds3 usaf-ds3  335000    67830", thrKey.toString());
        assertEquals("10 -9999  85000 -9999 -9999 -9999 -9999   320    61", thrLineValue.get(0).toString());
        assertEquals("10 -9999  70000 -9999 -9999 -9999 -9999   330    92", thrLineValue.get(1).toString());
	}
	
	private JobConf createConfig() {
        JobConf conf = new JobConf();
        System.out.println("start testing .txt files");
        conf.setStrings("mapreduce.input.fileinputformat.inputdir", "./fixtures/teste2.txt");
        return conf;
    }
	
	private JobConf creatConfigZip(){
		JobConf conf = new JobConf();
		System.out.println("start testing .zip files");
        conf.setStrings("mapreduce.input.fileinputformat.inputdir", "./fixtures/test.zip");
        return conf;
	}
}
