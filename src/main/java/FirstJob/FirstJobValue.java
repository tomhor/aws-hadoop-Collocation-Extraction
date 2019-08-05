package FirstJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;


public class FirstJobValue implements Writable {
	
	private LongWritable Cw1w2;
	private LongWritable Cw1;
	
	public FirstJobValue() {
		set(new LongWritable(0), new LongWritable(0));
	}
	
	public FirstJobValue(long Cw1w2, long Cw1) {
		set(new LongWritable(Cw1w2), new LongWritable(Cw1));
	}

	private void set(LongWritable Cw1w2, LongWritable Cw1) {
		this.Cw1w2 = Cw1w2;
		this.Cw1 = Cw1;
	}

	
	@Override
	public void write(DataOutput out) throws IOException {
		Cw1w2.write(out);
		Cw1.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Cw1w2.readFields(in);
		Cw1.readFields(in);
	}
	
	public LongWritable getCw1w2() {
		return Cw1w2;
	}
	
	public LongWritable getCw1() {
		return Cw1;
	}
    
    public String toString() {
        return "Cw1w2 " + this.Cw1w2.toString() + " Cw1 " + this.Cw1.toString();
    }
}
