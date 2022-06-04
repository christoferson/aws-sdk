package demo.aws.modules;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CryptoMaterialsManager;
import com.amazonaws.encryptionsdk.MasterKeyProvider;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.CryptoMaterialsCache;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;
import com.amazonaws.encryptionsdk.kms.KmsMasterKey;
import com.amazonaws.encryptionsdk.kms.KmsMasterKeyProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.ListAliasesRequest;
import com.amazonaws.services.kms.model.ListAliasesResult;
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
	
	public void aliasList(String keyId) {

		ListAliasesRequest req = new ListAliasesRequest().withKeyId(keyId).withLimit(3);
		ListAliasesResult result = client.listAliases(req);
		if (result.getAliases().isEmpty()) {
			System.out.println("KMS - No Alias to List");
		}
		for (var key : result.getAliases()) {
			System.out.println(key.getAliasName());
		}		
	}
	

    /*
     * Security thresholds
     *   Max entry age is required.
     *   Max messages (and max bytes) per data key are optional
     */
    private static final int MAX_ENTRY_MSGS = 100;

    public static byte[] encryptWithCaching(String kmsCmkArn, int maxEntryAge, int cacheCapacity, AWSCredentials credentials) {
        // Plaintext data to be encrypted
        byte[] myData = "My plaintext data".getBytes(StandardCharsets.UTF_8);

        // Encryption context
        // Most encrypted data should have an associated encryption context
        // to protect integrity. This sample uses placeholder values.
        // For more information see:
        // blogs.aws.amazon.com/security/post/Tx2LZ6WBJJANTNW/How-to-Protect-the-Integrity-of-Your-Encrypted-Data-by-Using-AWS-Key-Management
        final Map<String, String> encryptionContext = Collections.singletonMap("purpose", "test");

        // Create a master key provider
        MasterKeyProvider<KmsMasterKey> keyProvider = KmsMasterKeyProvider.builder()
        		.withCredentials(credentials)
        		.buildStrict(kmsCmkArn);

        // Create a cache
        CryptoMaterialsCache cache = new LocalCryptoMaterialsCache(cacheCapacity);

        // Create a caching CMM
        CryptoMaterialsManager cachingCmm =
                CachingCryptoMaterialsManager.newBuilder().withMasterKeyProvider(keyProvider)
                        .withCache(cache)
                        .withMaxAge(maxEntryAge, TimeUnit.SECONDS)
                        .withMessageUseLimit(MAX_ENTRY_MSGS)
                        
                        .build();

        // When the call to encryptData specifies a caching CMM,
        // the encryption operation uses the data key cache
        final AwsCrypto encryptionSdk = AwsCrypto.standard();
        return encryptionSdk.encryptData(cachingCmm, myData, encryptionContext).getResult();
    }
	
}
