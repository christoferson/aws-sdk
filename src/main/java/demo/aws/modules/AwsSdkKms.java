package demo.aws.modules;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoMaterialsManager;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.amazonaws.encryptionsdk.MasterKeyProvider;
import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.caching.CryptoMaterialsCache;
import com.amazonaws.encryptionsdk.caching.LocalCryptoMaterialsCache;
import com.amazonaws.encryptionsdk.kms.KmsMasterKey;
import com.amazonaws.encryptionsdk.kms.KmsMasterKeyProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClient;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.DecryptRequest;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.amazonaws.services.kms.model.InvalidCiphertextException;
import com.amazonaws.services.kms.model.ListAliasesRequest;
import com.amazonaws.services.kms.model.ListAliasesResult;
import com.amazonaws.services.kms.model.ListKeysRequest;
import com.amazonaws.services.kms.model.ListKeysResult;

public class AwsSdkKms {

	private AWSKMS client;
	
	private AWSCredentials credentials;
	
	private CryptoMaterialsCache cache;
	
	public AwsSdkKms(AWSCredentials credentials) {
		
		this.client = AWSKMSClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion(Regions.US_EAST_1)
				  .build();
	
		this.credentials = credentials;

        // Create a cache
        this.cache = new LocalCryptoMaterialsCache(20);


        
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

    public byte[] encryptWithCaching(String kmsCmkArn, String data) {
        // Plaintext data to be encrypted
        byte[] myData = data.getBytes(StandardCharsets.UTF_8);


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

        // Create a caching CMM
        CryptoMaterialsManager cachingCmm =
                CachingCryptoMaterialsManager.newBuilder()
                		.withMasterKeyProvider(keyProvider)
                        .withCache(cache)
                        .withMaxAge(300, TimeUnit.SECONDS)
                        .withMessageUseLimit(MAX_ENTRY_MSGS)
                        .build();

        // When the call to encryptData specifies a caching CMM,
        // the encryption operation uses the data key cache
        final AwsCrypto encryptionSdk = AwsCrypto.standard();
        
        return encryptionSdk.encryptData(cachingCmm, myData, encryptionContext).getResult();
    }
    
    public byte[] decrypt(final String keyArn, final byte[] ciphertext) {
        // 1. Instantiate the SDK
        // This builds the AwsCrypto client with the RequireEncryptRequireDecrypt commitment policy,
        // which enforces that this client only encrypts using committing algorithm suites and enforces
        // that this client will only decrypt encrypted messages that were created with a committing algorithm suite.
        // This is the default commitment policy if you build the client with `AwsCrypto.builder().build()`
        // or `AwsCrypto.standard()`.
        final AwsCrypto crypto = AwsCrypto.builder()
                .withCommitmentPolicy(CommitmentPolicy.RequireEncryptRequireDecrypt)
                .build();

        // 2. Instantiate an AWS KMS master key provider in strict mode using buildStrict().
        // In strict mode, the AWS KMS master key provider encrypts and decrypts only by using the key
        // indicated by keyArn.
        // To encrypt and decrypt with this master key provider, use an AWS KMS key ARN to identify the CMKs.
        // In strict mode, the decrypt operation requires a key ARN.
        final KmsMasterKeyProvider keyProvider = KmsMasterKeyProvider.builder()
        		.withCredentials(credentials)        		
        		.buildStrict(keyArn);


        // 5. Decrypt the data
        final CryptoResult<byte[], KmsMasterKey> decryptResult = crypto.decryptData(keyProvider, ciphertext);

        return decryptResult.getResult();

    }    
    
    
    //

	public static void demoAEADInvalidCiphertextException() {
		AWSKMS kms = AWSKMSClient.builder().build();

		String plaintext = "My very secret message";
		byte[] plaintextBytes = plaintext.getBytes(StandardCharsets.UTF_8);
		System.out.println("Plaintext: " + plaintext);

		// Encrypt the data
		EncryptRequest encReq = new EncryptRequest();
		encReq.setKeyId("alias/EcDemo");
		encReq.setPlaintext(ByteBuffer.wrap(plaintextBytes));
		ByteBuffer ciphertext = kms.encrypt(encReq).getCiphertextBlob();

		// Decrypt the data
		DecryptRequest decReq1 = new DecryptRequest();
		decReq1.setCiphertextBlob(ciphertext);
		ByteBuffer decrypted = kms.decrypt(decReq1).getPlaintext();
		String decryptedStr = new String(decrypted.array(), StandardCharsets.UTF_8);
		System.out.println("Decrypted: " + decryptedStr);

		// Attempt to tamper with the ciphertext
		byte[] tamperedCt = ciphertext.array().clone();
		// Flip all the bits in a byte 24 bytes from the end
		tamperedCt[tamperedCt.length - 24] ^= 0xff;

		DecryptRequest decReq2 = new DecryptRequest();
		decReq2.setCiphertextBlob(ByteBuffer.wrap(tamperedCt));

		try {
			kms.decrypt(decReq2).getPlaintext();
		} catch (InvalidCiphertextException ex) {
			ex.printStackTrace();
		}
	}

}
