package SecondJob;



import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class SecondJobRecordReader extends RecordReader<SecondJobKey, SecondJobValue> {
	
	SecondJobKey key;
	SecondJobValue value;
    LineRecordReader reader;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		reader.initialize(split, context);
	}
	
    public SecondJobRecordReader() {
        reader = new LineRecordReader(); 
    }

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (reader.nextKeyValue()) {
			/*
			 * "firstWord " + this.w1.toString() + " secondWord " + this.w2.toString() + " decade " + this.decade.toString();
			 * Cw1w2 " + this.Cw1w2.toString() + "Cw1 " + this.Cw1.toString()
			 */
			String[] line = (reader.getCurrentValue().toString()).split("\t");
			String[] keys = line[0].split(" ");
			String[] values = line[1].split(" ");
			String w1 = keys[1];
			String w2 = keys[3];
			int decade = Integer.parseInt(keys[5]);
			long Cw1w2 = Long.parseLong(values[1]);
			long Cw1 = Long.parseLong(values[3]);
			key = new SecondJobKey(w1,w2,decade);
			value = new SecondJobValue(Cw1w2,Cw1);
			return true;
		}return false;
	}

	@Override
	public SecondJobKey getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public SecondJobValue getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return reader.getProgress();
	}

	@Override
	public void close() throws IOException {
		reader.close();

	}

}
