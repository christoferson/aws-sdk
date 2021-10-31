package demo.aws.modules;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

public class AwsSdkSqs {
	
	private AmazonSQS client;
	
	public AwsSdkSqs(AWSCredentials credentials, Regions region) {
		
		client = AmazonSQSClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion((region == null)? Regions.US_EAST_1 : region)
				  .build();
	}
	
}
