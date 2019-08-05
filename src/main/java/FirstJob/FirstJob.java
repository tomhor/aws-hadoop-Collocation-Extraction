package FirstJob;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.StringTokenizer;
//import java.util.StringTokenizer;



public class FirstJob {

	 public static class FirstMapperClass extends Mapper<LongWritable,Text, FirstJobKey, LongWritable>{
		
		   
		    @Override
	        public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
		    	
	            StringTokenizer st = new StringTokenizer(value.toString());            
	            if(st.countTokens() < 4) return;
	            String ast = "*";
	            String w1 = st.nextToken().replaceAll("[^A-Za-z0-9]","").toLowerCase();
	            String w2 = st.nextToken().replaceAll("[^A-Za-z0-9]","").toLowerCase();
	            String yearString = st.nextToken();
	            int year = Integer.parseInt(yearString);
	            String amountString = st.nextToken();
	            long amount = Long.parseLong(amountString);
	            int decade = year - year % 10;
	            /*
	             * for each pair we write 4 jobs
	             * 1) <*,*,decade>: counting everything in the decade 
	             * 2) <firstWord,*,decade>: counting first word
	             * 3) <*,secWord,decade>: counting second word
	             * 4) <firstWord,secWord,decade>: counting both words together
	             */
	            context.write(new FirstJobKey(ast,ast,decade), new LongWritable(amount));// 1
	            context.write(new FirstJobKey(w1,ast,decade), new LongWritable(amount));// 2
	            context.write(new FirstJobKey(ast,w2,decade), new LongWritable(amount));// 3
	            context.write(new FirstJobKey(w1,w2,decade), new LongWritable(amount)); // 4
	    }
	 }

	    public static class FirstReducerClass extends Reducer<FirstJobKey,LongWritable,FirstJobKey, FirstJobValue> {

		    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
		    private long firstWordCount = 0;
		    private long counter = 0;
	    	
		    @Override
			public void reduce(FirstJobKey key, Iterable<LongWritable> Values, Context context) throws IOException, InterruptedException {
	        	long count = 0;
	        	for(LongWritable value : Values) {
	        		count += value.get();
	        	}
	        	this.counter = count;
	            String ast = "*";
	            String w1 = key.getW1().toString();
	            String w2 = key.getW2().toString();
	            int decade = key.getDecade().get();
	            // check if <*,*> , firstWord == secWord == "*"
	            if(w1.equals(ast) && w2.equals(ast)) {
		        	String file = String.valueOf(count);
	            	String Nfolder = context.getConfiguration().get("Nfolder");
	                InputStream is = new ByteArrayInputStream(file.getBytes());
	                ObjectMetadata metadata = new ObjectMetadata();
	                metadata.setContentLength(file.getBytes().length);
	                String fileName = String.valueOf(decade) + " " + file;
	    	        PutObjectRequest req = new PutObjectRequest(Nfolder, fileName, is ,metadata);       
	                s3.putObject(req);  
	            }else if(w1.equals(ast)) {// check if <*, secWord > , firstWord = *	
	        		FirstJobKey newFirstKey = new FirstJobKey(w2,ast,decade);
	        		FirstJobValue newFirstValue = new FirstJobValue(0, counter);
	        		context.write(newFirstKey,newFirstValue);
	        	} else if(w2.equals(ast)) {// check if <firstWord,*> , SectWord = *
	        		this.firstWordCount = count;
	        	} else { // <firstWord,secWord>
	        		FirstJobKey newKey = new FirstJobKey(w2,w1, decade);
	        		FirstJobValue newValue = new FirstJobValue(counter, firstWordCount);
	        		context.write(newKey, newValue);
	        	}
	        }
	    }
	    
	    public static class FirstPartitionerClass extends Partitioner< FirstJobKey, LongWritable> {
			@Override
			public int getPartition(FirstJobKey key, LongWritable value, int numPartitions) {
				return Math.abs(key.hashCodeReducer()) % numPartitions;
			}
	    }

	    public static class FirstCombinerClass extends Reducer<FirstJobKey,LongWritable,FirstJobKey,LongWritable> {
	    	
	    	@Override
	        public void reduce(FirstJobKey key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
	        	long valueSum = 0;
	        	for(LongWritable value : values) {
	        		valueSum += value.get();
	        	}
	        	context.write(key, new LongWritable(valueSum));
	        }
	    }

	    

	    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	        String uuid = args[0];
	        String bucketName = args[1]; // bucket SSL 
	        String inputPath = args[2]; // input file is the data
	        String workingFolder = bucketName + "/" + uuid ;
	        String outputPath =  "s3n://" + workingFolder + "/step1";
	        String Nfolder = workingFolder + "/N"; // N folder
	    	Configuration conf = new Configuration();
	    	conf.set("Nfolder", Nfolder);
	        Job job = Job.getInstance(conf,"FirstJob");
	        job.setJarByClass(FirstJob.class);//Jar class
	        job.setPartitionerClass(FirstPartitionerClass.class);
	        job.setMapperClass(FirstMapperClass.class);
	        job.setReducerClass(FirstReducerClass.class);
	        job.setCombinerClass(FirstCombinerClass.class);
	        job.setMapOutputKeyClass(FirstJobKey.class);
	        job.setMapOutputValueClass(LongWritable.class);
//	        job.setNumReduceTasks(1);//how many reduce tasks that we want
	        job.setInputFormatClass(SequenceFileInputFormat.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
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