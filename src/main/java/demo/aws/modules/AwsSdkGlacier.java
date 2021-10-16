package demo.aws.modules;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;

public class AwsSdkGlacier {

	private AmazonGlacier client;
	
	public AwsSdkGlacier(AWSCredentials credentials) {
		
		client = AmazonGlacierClientBuilder
				.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion(Regions.US_EAST_1).build();

	}
	
	public void upload() {
		
		//client.uploadArchive(null);
		
//        ArchiveTransferManager atm = new ArchiveTransferManager(client, credentials);
//        
//        UploadResult result = atm.upload(vaultName, "my archive " + (new Date()), new File(archiveToUpload));
//        System.out.println("Archive ID: " + result.getArchiveId());
        
	}
	
}
