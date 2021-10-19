package demo.aws.modules;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.ListKeysRequest;
import com.amazonaws.services.kms.model.ListKeysResult;

public class AwsSdkKms {

	private AWSKMS client;
	
	public AwsSdkKms(AWSCredentials credentials) {
		
		this.client = AWSKMSClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion(Regions.US_EAST_1)
				  .build();
	}
	
	public void list() {

		ListKeysRequest req = new ListKeysRequest().withLimit(10);
		ListKeysResult result = client.listKeys(req);
		if (result.getKeys().isEmpty()) {
			System.out.println("KMS - No Keys to List");
		}
		for (var key : result.getKeys()) {
			System.out.println(key.getKeyArn());
		}
	}
	
}
