package demo.aws.modules;

import java.net.URL;
import java.util.List;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PublicAccessBlockConfiguration;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SetPublicAccessBlockRequest;
import com.amazonaws.services.s3.model.SetPublicAccessBlockResult;

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
	
	public void presign(String bucketname, String objectname, HttpMethod method) {
		
		// Set the pre-signed URL to expire after one hour.
        java.util.Date expiration = new java.util.Date();
        long expTimeMillis = expiration.getTime();
        expTimeMillis += 1000 * 60; //expTimeMillis += 1000 * 60 * 60;
        expiration.setTime(expTimeMillis);

        // Generate the pre-signed URL.
        System.out.println("Generating pre-signed URL.");
        GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketname, objectname)
                .withMethod(method)
                .withExpiration(expiration);
        URL url = s3client.generatePresignedUrl(generatePresignedUrlRequest);
        System.out.println(url);
	}
	
	public void url(String bucketname, String objectname) {

		URL url = s3client.getUrl(bucketname, objectname);
		System.out.println(url);
	}
	
	public void exists(String bucketname) {

		boolean exists = s3client.doesBucketExistV2(bucketname);
		System.out.println(exists);
	}
	
	public void create(String bucketname, Region region) {

		CreateBucketRequest request = new CreateBucketRequest(bucketname, region).withCannedAcl(CannedAccessControlList.Private);
		
		Bucket bucket = s3client.createBucket(request);
		System.out.println(bucket);
		
		// Block all public access
		SetPublicAccessBlockRequest r2 = new SetPublicAccessBlockRequest();
		r2.withBucketName(bucketname).withPublicAccessBlockConfiguration(
				new PublicAccessBlockConfiguration()
					.withBlockPublicAcls(true)
					.withBlockPublicPolicy(true)
					.withIgnorePublicAcls(true)
					.withRestrictPublicBuckets(true));
		SetPublicAccessBlockResult r2r = s3client.setPublicAccessBlock(r2);
		System.out.println(r2r);
	}
	
}
