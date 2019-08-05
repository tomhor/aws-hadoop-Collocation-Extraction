package FirstJob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class FirstJobKey implements WritableComparable<FirstJobKey>{
	
	private IntWritable decade;
	private Text w1;
	private Text w2;

	
	public FirstJobKey() {
		set(new Text(), new Text(), new IntWritable());
	}
	
	public FirstJobKey(String w1, String w2, int decade) {
		set(new Text(w1), new Text(w2), new IntWritable(decade));
	}


	private void set(Text w1, Text w2, IntWritable decade) {
		this.w1 = w1;
		this.w2 = w2;
		this.decade = decade;
	}
	
	public Text getW1() {
		return this.w1;
	}
	
	public Text getW2() {
		return this.w2;
	}
	
	public IntWritable getDecade() {
		return this.decade;
	}
	

	public int hashCodeReducer() {		
		return this.decade.hashCode() + this.w1.hashCode();
	}
	

	@Override
	public void write(DataOutput out) throws IOException {
		w1.write(out);
		w2.write(out);
		decade.write(out);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		w1.readFields(in);
		w2.readFields(in);
		decade.readFields(in);
	}

	public int compareTo(FirstJobKey o) {
		int decade = this.decade.get();
		int Odecade = o.getDecade().get();
		String w1 = this.w1.toString();
		String Ow1 = o.getW1().toString();
		String w2 = this.w2.toString();
		String Ow2 = o.getW2().toString();
		String ast = "*";
		if(decade == Odecade) {
		if(w1.equals(ast) && !Ow2.equals(ast)) return -1;
		else if(!w1.equals(ast) && Ow2.equals(ast))	return 1;
		else { 
			if(w1.equals(Ow1)) {
				if(w2.equals(ast) && !Ow2.equals(ast)) return -1;
				else if(!w2.equals(ast) && Ow2.equals(ast))return 1;
				else return w2.compareTo(Ow2);
			} else return w1.compareTo(Ow2);
			}
		} else {
			return this.decade.compareTo(o.getDecade());
		}	
	}
		
//		if(decade == Odecade){
//			if(w1.equals(ast) && !Ow1.equals(ast)) return -1; // <*,> <[],>
//			else if(!w1.equals(ast) && Ow1.equals(ast)) return 1; //<[],> <*,>
//			else if(!w1.equals(ast) && !Ow1.equals(ast)) { // <[],> <[],>
//				if(w2.equals(ast) && !Ow2.equals(ast)) return 1; // <*,> <[],>
//				else if(!w2.equals(ast) && Ow2.equals(ast)) return -1; //<[],> <*,>
//				else return w2.compareTo(Ow2);
//			}else  // <*,> <*,>
//				return w2.compareTo(Ow2);
//		}return decade - Odecade;
		
//        if(this.decade.get() - o.getDecade().get() == 0) {
//            if (this.w1.toString().compareTo(o.getW1().toString()) == 0)
//                return this.w2.toString().compareTo(o.getW2().toString());
//            return  this.w1.toString().compareTo(o.getW1().toString());
//        }
//        return this.decade.get() - o.getDecade().get();
//	}


    public String toString() {
        return "firstWord " + this.w1.toString() + " secondWord " + this.w2.toString() + " decade " + this.decade.toString();
    }


}
