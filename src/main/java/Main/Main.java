package Main;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import java.io.IOException;
import java.util.UUID;


public class Main {


    public static void main(String[] args) throws IOException {
    	if(args.length != 2) {
    		System.out.println("args must be 2");
    		System.exit(2);
    	}
        UUID uuid = UUID.randomUUID();
        String bucketName = "tom-amiram";
   //     String input = "s3n://tom-amiram/eng-2gram";
        String input = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
        AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(credentialsProvider)
                .build();


        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
               .withJar("s3n://"+ bucketName +"/firstjob.jar") // This should be a full map reduce application.
                .withArgs(uuid.toString(), bucketName , input);

        StepConfig stepConfig1 = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://"+ bucketName +"/secondjob.jar") // This should be a full map reduce application.
                .withArgs(uuid.toString(),bucketName);

        StepConfig stepConfig2 = new StepConfig()
                .withName("step2")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://"+ bucketName +"/thirdjob.jar") // This should be a full map reduce application.
                .withArgs(uuid.toString(),bucketName,args[0],args[1]);

        StepConfig stepConfig3 = new StepConfig()
                .withName("step3")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withMasterInstanceType(InstanceType.M1Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M1Xlarge.toString())
                .withHadoopVersion("2.2.0")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withEc2KeyName("bullpup2005")
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("assignment2")
                .withReleaseLabel("emr-5.14.0")
                .withInstances(instances)
                .withSteps(stepConfig1,stepConfig2,stepConfig3)
                .withLogUri("s3n://"+ bucketName +"/"+uuid+"/logs/")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("minimal PMI:" + args[0]);
        System.out.println("Relative minimal PMI:" + args[1]);
        System.out.println("UUID: "+ uuid);
        System.out.println("flow ID: " + jobFlowId);

    }
}
