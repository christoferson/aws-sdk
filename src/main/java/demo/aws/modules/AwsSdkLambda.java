package demo.aws.modules;

import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.FunctionConfiguration;
import com.amazonaws.services.lambda.model.ListFunctionsResult;
import com.amazonaws.services.lambda.model.ServiceException;

public class AwsSdkLambda {

	private AWSLambda client;
	
	public AwsSdkLambda(AWSCredentials credentials, Regions region) {
		
		this.client = AWSLambdaClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion(region)
				  .build();
	}
	
	public void list() {

        ListFunctionsResult functionResult = null;

        try {

            functionResult = client.listFunctions();

            List<FunctionConfiguration> list = functionResult.getFunctions();

            for (FunctionConfiguration config : list) {
                System.out.println("The function name is " + config.getFunctionName());
            }

        } catch (ServiceException e) {
            System.out.println(e);
        }
        
	}
	
	
}
