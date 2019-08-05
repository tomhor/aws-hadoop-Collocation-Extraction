package ThirdJob;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;



public class ThirdJobRecordReader extends RecordReader<ThirdJobKey, ThirdJobValue> {
	
	ThirdJobKey key;
	ThirdJobValue value;
    LineRecordReader reader;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		reader.initialize(split, context);
	}
	
    public ThirdJobRecordReader() {
        reader = new LineRecordReader(); 
    }

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (reader.nextKeyValue()) {
			/*
			 * firstWord upon secondWord 100000 decade 2000	N 61 Cw1 1 Cw2 36 Cw1w2 2
			 */
			String[] line = (reader.getCurrentValue().toString()).split("\t");
			String[] keys = line[0].split(" ");
			String[] values = line[1].split(" ");
			String w1 = keys[1];
			String w2 = keys[3];
			int decade = Integer.parseInt(keys[5]);
			long N = Long.parseLong(values[1]);
			long Cw1 = Long.parseLong(values[3]);
			long Cw2 = Long.parseLong(values[5]);
			long Cw1w2 = Long.parseLong(values[7]);

			key = new ThirdJobKey(w1,w2,decade);
			value = new ThirdJobValue(N,Cw1,Cw2,Cw1w2);
			return true;
		}return false;
	}

	@Override
	public ThirdJobKey getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public ThirdJobValue getCurrentValue() throws IOException, InterruptedException {
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
