package demo.aws.modules;

import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class AwsSdkSqs {
	
	private AmazonSQS client;
	
	public AwsSdkSqs(AWSCredentials credentials, Regions region) {
		
		client = AmazonSQSClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion((region == null)? Regions.US_EAST_1 : region)
				  .build();
	}

	public void queueList() {
		
		ListQueuesResult result = client.listQueues();
		System.out.println("Your SQS Queue URLs:");
		for (String url : result.getQueueUrls()) {
		    System.out.println(url);
		}

	}

	public void queueGetUrl(String name) {
		
		String queue_url = client.getQueueUrl(name).getQueueUrl();
		System.out.println(String.format("Queue:%s URL:%s", name, queue_url));
	}
	
	public void messageReceive(String queueUrl) {
		
		System.out.println(String.format("Queue:%s Receiving Message...", queueUrl));
		
		ReceiveMessageRequest request = new ReceiveMessageRequest()
				.withQueueUrl(queueUrl);
				//.withWaitTimeSeconds(5)
		List<Message> messages = client.receiveMessage(request).getMessages();
		
		System.out.println(String.format("Queue:%s Received Message: %s", queueUrl, messages));
	}
	
	public void messageSend(String queueUrl) {
		
		SendMessageRequest request = new SendMessageRequest()
		        .withQueueUrl(queueUrl)
		        .withMessageBody("hello")
		        .withDelaySeconds(5);
		client.sendMessage(request);
		
	}
}
