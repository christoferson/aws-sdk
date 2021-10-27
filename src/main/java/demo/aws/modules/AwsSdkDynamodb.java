package demo.aws.modules;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

public class AwsSdkDynamodb {

	private AmazonDynamoDB client;
	
	public AwsSdkDynamodb(AWSCredentials credentials) {
		this(credentials, Regions.US_EAST_1);
	}

	public AwsSdkDynamodb(AWSCredentials credentials, Regions region) {
		
		this.client = AmazonDynamoDBClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion(region)
				  .build();
	}
	
	public void tableList() {
		
		DynamoDB dynamoDB = new DynamoDB(client);

		TableCollection<ListTablesResult> tables = dynamoDB.listTables();

		System.out.println("Listing Dynamo Tables");
        for (var table : tables) {
        	System.out.println(table.getTableName());
        }
 
	}
		
}
