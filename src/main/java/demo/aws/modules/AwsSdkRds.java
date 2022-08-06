package demo.aws.modules;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.DescribeDBInstancesRequest;
import com.amazonaws.services.rds.model.DescribeDBInstancesResult;

public class AwsSdkRds {
	
	private AmazonRDS client;
	
	public AwsSdkRds(AWSCredentials credentials) {
		
		client = AmazonRDSClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion(Regions.US_EAST_1)
				  .build();
	}
	
	public void instanceDescribe(String dbInstanceIdentifier) {
		
		DescribeDBInstancesRequest request = new DescribeDBInstancesRequest();
		request.setDBInstanceIdentifier(dbInstanceIdentifier);
		
		DescribeDBInstancesResult result = client.describeDBInstances(request);
		
		List<DBInstance> elements = result.getDBInstances();
        for (DBInstance element : elements) {
            System.out.println(String.format("Arn=%s %n  Name=%s Status=%s", element.getDBInstanceArn(), element.getDBName(), element.getDBInstanceStatus()));
        }
		
	}	
}
