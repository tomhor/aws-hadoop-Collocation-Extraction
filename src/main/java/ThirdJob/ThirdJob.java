package ThirdJob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ThirdJob {
	 public static class ThirdMapperClass extends Mapper<ThirdJobKey,ThirdJobValue, ThirdJobKey, DoubleWritable>{
	        public void map(ThirdJobKey key, ThirdJobValue value, Context context) throws IOException, InterruptedException {
	        
	        	double N = value.getN().get();
	        	double Cw1w2 = value.getCw1w2().get();
	        	double Cw1 = value.getCw1().get();
	        	double Cw2 = value.getCw2().get();	        
	        	double Pw1w2 = -1 * Math.log10(Cw1w2 / N);
	    	    if (Pw1w2 == 0.0) Pw1w2 = 0.01;
	    		double PMIw1w2 = Math.log10(Cw1w2) + Math.log10(N) - Math.log10(Cw1) - Math.log10(Cw2);
	    		double NPMIw1w2 = (PMIw1w2 / Pw1w2);
	    		if(key.getW1().toString().equals("*"))
	    			context.write(key, new DoubleWritable(Cw1w2));
	    		else context.write(key, new DoubleWritable(NPMIw1w2));
	        }
	 }
	 
	    public static class ThirdReducerClass extends Reducer<ThirdJobKey,DoubleWritable,Text, Text> {

	    	private long sumNpmi = 1000;
	    	private double minPmi;
	    	private double RminPmi;

			public void reduce(ThirdJobKey key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
				String ast = "*";
				String w1 = key.getW1().toString();
	        	minPmi = Double.valueOf(context.getConfiguration().get("minPmi"));
	            RminPmi = Double.valueOf(context.getConfiguration().get("RminPmi"));
	            long sum = 0;
	            	for (DoubleWritable value : values) {
	    	        	if(w1.equals(ast)) {
	    	            	sum += value.get();
	    	            	this.sumNpmi = sum;
	    	        	}else{
	    	        		double npmiValue = value.get();
	    	        		double relMinPmiValue = npmiValue / this.sumNpmi;
	    	        		if(npmiValue >= this.minPmi || relMinPmiValue >= this.RminPmi) 
	    	        			context.write(new Text(key.toString()), new Text("npmi: " + npmiValue + ", relMinNmpi: " + relMinPmiValue));
	    	        	}
	            	}
			}	
	    }
	    
	    public static class ThirdPartitionerClass extends Partitioner< ThirdJobKey, DoubleWritable> {
			public int getPartition(ThirdJobKey key, DoubleWritable value, int numPartitions) {
				return Math.abs(key.hashCodeReducer()) % numPartitions;
			}
	    }	    
	    
	    
    public static class ThirdCombinerClass extends Reducer<ThirdJobKey,DoubleWritable,ThirdJobKey,DoubleWritable> {
	    	
	        public void reduce(ThirdJobKey key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
	        	double valueSum = 0;
	        	for(DoubleWritable value : values) {
	        		valueSum = valueSum + value.get();
	        	}
	        	context.write(key, new DoubleWritable(valueSum));
	        }
	    }


	    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	        String uuid = args[0];
	        String bucketName = args[1]; // bucket SSL 
	        String minPmi = args[2];
	        String RminPmi = args[3];
	        String workingFolder = bucketName + "/" + uuid ;
	        String inputPath = "s3n://" + workingFolder + "/step2";
	        String outputPath =  "s3n://" + workingFolder + "/step3";
	    	Configuration conf = new Configuration();
	    	conf.set("minPmi", minPmi);
	        conf.set("RminPmi", RminPmi);
	        Job job = Job.getInstance(conf,"ThirdJob");
	        job.setJarByClass(ThirdJob.class);//Jar class
	        job.setPartitionerClass(ThirdPartitionerClass.class);
	        job.setMapperClass(ThirdMapperClass.class);
	        job.setReducerClass(ThirdReducerClass.class);
	        job.setCombinerClass(ThirdCombinerClass.class);
	        job.setMapOutputKeyClass(ThirdJobKey.class);
	        job.setMapOutputValueClass(DoubleWritable.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        job.setInputFormatClass(ThirdJobInputFormat.class);

	        FileInputFormat.addInputPath(job,new Path(inputPath));
	        FileOutputFormat.setOutputPath(job, new Path(outputPath));
	        boolean completion = job.waitForCompletion(true);
	        if(completion){
	            System.exit(0);
	        }else{
	            System.exit(1);
	        }
	    }
	    
}