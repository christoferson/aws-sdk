package demo.aws.modules;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

public class AwsSdkDynamodb {

	private AmazonDynamoDB client;
	
	private DynamoDB dynamoDB;
	
	public AwsSdkDynamodb(AWSCredentials credentials) {
		this(credentials, Regions.US_EAST_1);
	}

	public AwsSdkDynamodb(AWSCredentials credentials, Regions region) {
		
		this.client = AmazonDynamoDBClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion(region)
				  .build();
		
		dynamoDB = new DynamoDB(client);
	}
	
	public void tableList() {

		TableCollection<ListTablesResult> tables = dynamoDB.listTables();

		System.out.println("Listing Dynamo Tables");
        for (var table : tables) {
        	System.out.println(table.getTableName());
        }
 
	}
	
	public void tableDescribe(String tableName) {

        System.out.println("Describing " + tableName);

        TableDescription tableDescription = dynamoDB.getTable(tableName).describe();
        System.out.format(
            "Name: %s:\n" + "Status: %s \n" + "Provisioned Throughput (read capacity units/sec): %d \n"
                + "Provisioned Throughput (write capacity units/sec): %d \n",
            tableDescription.getTableName(), tableDescription.getTableStatus(),
            tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
            tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
    }
	
	
	public void itemRetrieve(String tableName, String keyRegionId, String keyPlayerId) {
        Table table = dynamoDB.getTable(tableName);

        try {
        	GetItemSpec i = new GetItemSpec();
        	i.withPrimaryKey("Region", keyRegionId, "PlayerID", keyPlayerId);
            Item item = table.getItem("Region", keyRegionId, "PlayerID", keyPlayerId, "PlayerID", null);
            
            System.out.println("Printing item after retrieving it....");
            //System.out.println(item.toJSONPretty());
            System.out.println(item);

        }
        catch (Exception e) {
            System.err.println("GetItem failed.");
            System.err.println(e.getMessage());
        }

    }
	
    public void itemRegister(String tableName, String keyRegionId, String keyPlayerId) {

        Table table = dynamoDB.getTable(tableName);
        try {

            Item item = new Item()
            		.withPrimaryKey("Region", keyRegionId, "PlayerID", keyPlayerId)
            		.withString("Level", "1");
            table.putItem(item, "attribute_not_exists(PlayerID)", null, null);

            System.out.println("Registered: " + item);

        } catch (Exception e) {
            System.err.println("Create items failed.");
            System.err.println(e.getMessage());
        }

    }

}
