package demo.aws.modules;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.amazonaws.services.glacier.model.ListVaultsRequest;
import com.amazonaws.services.glacier.model.ListVaultsResult;

public class AwsSdkGlacier {

	private AmazonGlacier client;
	
	public AwsSdkGlacier(AWSCredentials credentials) {
		
		client = AmazonGlacierClientBuilder
				.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion(Regions.US_EAST_1).build();

	}
	
	public void vaultList() {
		ListVaultsRequest request = new ListVaultsRequest();
		request.withAccountId("-").withLimit("1");
		ListVaultsResult result = client.listVaults(request);
		if (result.getVaultList().isEmpty()) {
			System.out.println("No Vaults to List");
		}
		for (var vault : result.getVaultList()) {
			System.out.println(vault.getVaultName());
		}
	}
	
	public void upload() {
		
		//client.uploadArchive(null);
		
//        ArchiveTransferManager atm = new ArchiveTransferManager(client, credentials);
//        
//        UploadResult result = atm.upload(vaultName, "my archive " + (new Date()), new File(archiveToUpload));
//        System.out.println("Archive ID: " + result.getArchiveId());
        
	}
	
}
