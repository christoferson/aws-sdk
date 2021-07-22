package demo.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import demo.aws.modules.AwsSdkS3;

public class AwsSdk {

	public static void main(String[] args) {
		
		AWSCredentials credentials = new BasicAWSCredentials("...", "...");
		
		AwsSdkS3 s3 = new AwsSdkS3(credentials);
		s3.buckets();

	}

}
