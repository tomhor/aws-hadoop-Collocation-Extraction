package SecondJob;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class SecondJob {
	 public static class SecondMapperClass extends Mapper<SecondJobKey,SecondJobValue, SecondJobKey, SecondJobValue>{
	        public void map(SecondJobKey key,SecondJobValue value, Context context) throws IOException, InterruptedException {
	        	context.write(key, value);
	        }
	    }

	    public static class SecondReducerClass extends Reducer<SecondJobKey,SecondJobValue,SecondJobKey, SecondJobValueRed> {

		    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
		    private long amount;
		    private HashMap<Integer,Long> decades; 

		    public void setup(Context context) {
				decades = new HashMap<Integer,Long>();
				String Nfolder = context.getConfiguration().get("Nfolder");
				String bucketName = context.getConfiguration().get("bucketName");
		        ObjectListing olist = s3.listObjects(bucketName, Nfolder);
		        for (S3ObjectSummary summary : olist.getObjectSummaries()) {
		        	String filePath = summary.getKey();
		        	String[] filePathArr = filePath.split("/");
		        	if(filePathArr.length > 1) {
		        		String filename = filePath.split("/")[2];
		 	            int decade = Integer.parseInt((filename.split(" ")[0]));
		 	            long N = Long.parseLong((filename.split(" ")[1]));
		 	            decades.put(decade, N);

		        	}
		        }
	    	}

			public void reduce(SecondJobKey key, Iterable<SecondJobValue> values, Context context) throws IOException, InterruptedException {
				String ast = "*";
				String w1 = key.getW1().toString();
				String w2 = key.getW2().toString();
				long count = 0;
				if(w2.equals(ast)) { // check if <firstWord,*> , SecWord = *
	            	for (SecondJobValue value : values) {
	            		count += value.getCw1().get();
	            	}
	            	amount = count;
	        	} else 
	        	
	        	{ // <firstWord,SecondWord>
	        		if(decades.containsKey(key.getDecade().get())) {
	        			long Cw1w2 = 0;
	        			long Cw1 = 0;
	        			for (SecondJobValue value : values) {
	        				Cw1w2 += value.getCw1w2().get();
	        				Cw1 = value.getCw1().get();
	        			}
	        			int decade = key.getDecade().get();
	        			long N = this.decades.get(decade);
	        			long Cw2 = this.amount;
	        			SecondJobValueRed secondValue = new SecondJobValueRed(N,Cw1,Cw2,Cw1w2);
	        			SecondJobKey secondKey = new SecondJobKey(w2,w1,decade);
	        			context.write(secondKey, secondValue);
	        			context.write(new SecondJobKey(ast,ast, decade), secondValue);
	        		}
	        	}
			}
			
	    }
	    
	    public static class SecondPartitionerClass extends Partitioner< SecondJobKey, SecondJobValue> {
			public int getPartition(SecondJobKey key, SecondJobValue value, int numPartitions) {
				return Math.abs(key.hashCodeReducer()) % numPartitions;
			}
	    }	    

	    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	        String uuid = args[0];
	        String bucketName = args[1]; // bucket SSL 
	        String workingFolder = bucketName + "/" + uuid ;
	        String inputPath = "s3n://" + workingFolder + "/step1";
	        String outputPath =  "s3n://" + workingFolder + "/step2";
	        String Nfolder = uuid + "/N"; // N folder
	    	Configuration conf = new Configuration();
	    	conf.set("Nfolder", Nfolder);
	    	conf.set("bucketName", bucketName);
	        Job job = Job.getInstance(conf,"SecondJob");
	        job.setJarByClass(SecondJob.class);
	        job.setPartitionerClass(SecondPartitionerClass.class);
	        job.setMapperClass(SecondMapperClass.class);
	        job.setReducerClass(SecondReducerClass.class);
	        job.setMapOutputKeyClass(SecondJobKey.class);
	        job.setMapOutputValueClass(SecondJobValue.class);
	        job.setOutputKeyClass(SecondJobKey.class);
//	        job.setNumReduceTasks(1);//how many reduce tasks that we want
	        job.setOutputValueClass(SecondJobValueRed.class);
	        job.setInputFormatClass(SecondJobInputFormat.class);
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