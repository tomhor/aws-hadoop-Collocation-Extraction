package SecondJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;


public class SecondJobValueRed implements Writable{
	
	private LongWritable N;
	private LongWritable Cw1;
	private LongWritable Cw2;
	private LongWritable Cw1w2;


	
	public SecondJobValueRed() {
		set(0,0,0,0);
	}
	
	public SecondJobValueRed(long N,long Cw1, long Cw2, long Cw1w2) {
		set(N,Cw1,Cw2,Cw1w2);
	}


	private void set(long N,long Cw1, long Cw2, long Cw1w2) {
		this.N = new LongWritable(N);
		this.Cw1 = new LongWritable(Cw1);
		this.Cw2 = new LongWritable(Cw2);
		this.Cw1w2 = new LongWritable(Cw1w2);

	}
	public LongWritable getN() {
		return this.N;
	}
	
	public LongWritable getCw1w2() {
		return this.Cw1w2;
	}
	
	public LongWritable getCw1() {
		return this.Cw1;
	}
	
	public LongWritable getCw2() {
		return this.Cw2;
	}
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.N == null) ? 0 : this.N.hashCode());
		result = prime * result + ((this.Cw1 == null) ? 0 : this.Cw1.hashCode());
		result = prime * result + ((this.Cw2 == null) ? 0 : this.Cw2.hashCode());
		result = prime * result + ((this.Cw1w2 == null) ? 0 : this.Cw1w2.hashCode());
		return result;
	}
	

	@Override
	public void write(DataOutput out) throws IOException {
		N.write(out);
		Cw1.write(out);
		Cw2.write(out);		
		Cw1w2.write(out);		

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Cw1.readFields(in);
		Cw2.readFields(in);
		Cw1w2.readFields(in);
		N.readFields(in);
	}


    public String toString() {
        return "N " + this.N.toString() + " Cw1 " + this.Cw1.toString() + " Cw2 " + this.Cw2.toString()+ " Cw1w2 " + this.Cw1w2.toString();
    }




}
