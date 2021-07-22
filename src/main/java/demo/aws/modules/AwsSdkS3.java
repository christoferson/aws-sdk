package demo.aws.modules;

import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AwsSdkS3 {
	
	private AmazonS3 s3client;
	
	public AwsSdkS3(AWSCredentials credentials) {
		
		s3client = AmazonS3ClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion(Regions.US_EAST_1)
				  .build();
	}
	
	public void buckets() {

		List<Bucket> buckets = s3client.listBuckets();
		for (Bucket bucket : buckets) {
		    System.out.println(bucket.getName());
		}
		
	}
	
	public void objects(String bucketname) {

		ObjectListing objectListing = s3client.listObjects(bucketname);
		for(S3ObjectSummary os : objectListing.getObjectSummaries()) {
			System.out.println(String.format("%s %s %s", os.getKey(), os.getLastModified(), os.getStorageClass()));
		}
		
	}
	
}
