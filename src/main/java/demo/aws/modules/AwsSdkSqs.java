package demo.aws.modules;

import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;

public class AwsSdkSqs {
	
	private AmazonSQS client;
	
	public AwsSdkSqs(AWSCredentials credentials, Regions region) {
		
		client = AmazonSQSClientBuilder
				  .standard()
				  .withCredentials(new AWSStaticCredentialsProvider(credentials))
				  .withRegion((region == null)? Regions.US_EAST_1 : region)
				  .build();
	}

	public void queueCreate(String queueName) {
		
		try {
			CreateQueueResult result = client.createQueue(queueName);
			System.out.println(String.format("Queue:%s Created Queue. URL:%s", queueName, result.getQueueUrl()));
		} catch (AmazonSQSException e) {
		    if (!e.getErrorCode().equals("QueueAlreadyExists")) {
		        throw e;
		    }
		}
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
	
	public void queueGetArn(String queueUrl) {
		
		GetQueueAttributesResult queue_attrs = client.getQueueAttributes(
		        new GetQueueAttributesRequest(queueUrl)
		            .withAttributeNames("QueueArn"));

		String arn = queue_attrs.getAttributes().get("QueueArn");
		System.out.println(String.format("Queue:%s ARN:%s", queueUrl, arn));
		
	}
	
	public void queueSetDeadLetterQueue(String queueName, String deadLetterQueueName) {
		
		String deadLetterQueueUrl = client.getQueueUrl(deadLetterQueueName).getQueueUrl();

		GetQueueAttributesResult queue_attrs = client.getQueueAttributes(
				new GetQueueAttributesRequest(deadLetterQueueUrl).withAttributeNames("QueueArn"));
		
		String deadLetterQueueArn = queue_attrs.getAttributes().get("QueueArn");
		
		//Set dead letter queue with redrive policy on source queue.
		String queueUrl = client.getQueueUrl(queueName).getQueueUrl();
		
		SetQueueAttributesRequest request = new SetQueueAttributesRequest()
		.withQueueUrl(queueUrl)
		.addAttributesEntry("RedrivePolicy",
		       "{\"maxReceiveCount\":\"5\", \"deadLetterTargetArn\":\""
		       + deadLetterQueueArn + "\"}");
		
		client.setQueueAttributes(request);

	}
	
	public void messageReceive(String queueUrl) {
		
		System.out.println(String.format("Queue:%s Receiving Message...", queueUrl));
		
		ReceiveMessageRequest request = new ReceiveMessageRequest()
				.withQueueUrl(queueUrl)
				.withMaxNumberOfMessages(10);
				//.withWaitTimeSeconds(5)
		List<Message> messages = client.receiveMessage(request).getMessages();
		for (Message m : messages) {
			System.out.println(String.format("Queue:%s Received Message: %s", queueUrl, m));
		}

		for (Message m : messages) {
		    client.changeMessageVisibility(queueUrl, m.getReceiptHandle(), 0);
		}
		
		//for (Message m : messages) {
		//    client.deleteMessage(queueUrl, m.getReceiptHandle());
		//}
		
	}
	
	public void messageReceiveLongPolling(String queueUrl) {
		
		System.out.println(String.format("Queue:%s Receiving Message...", queueUrl));
		
		ReceiveMessageRequest request = new ReceiveMessageRequest()
				.withQueueUrl(queueUrl)
				.withMaxNumberOfMessages(10)
				.withWaitTimeSeconds(10);
		List<Message> messages = client.receiveMessage(request).getMessages();
		
		for (Message m : messages) {
			System.out.println(String.format("Queue:%s Received Message: %s", queueUrl, m));
		}
		// Delete Messages after Receipt
		for (Message m : messages) {
			client.deleteMessage(queueUrl, m.getReceiptHandle());
		}
		
	}
	
	public void messageSend(String queueUrl) {
		
		SendMessageRequest request = new SendMessageRequest()
		        .withQueueUrl(queueUrl)
		        .withMessageBody("hello")
		        //.withDelaySeconds(5)
		        ;
		SendMessageResult result = client.sendMessage(request);
		
		System.out.println(String.format("Queue:%s Set Message: %s", queueUrl, result.getMessageId()));
		
	}
	
	
	
}
