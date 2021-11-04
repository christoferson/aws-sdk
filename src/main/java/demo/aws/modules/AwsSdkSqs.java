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

	public String queueGetUrl(String name) {
		
		String queueUrl = client.getQueueUrl(name).getQueueUrl();
		System.out.println(String.format("Queue:%s URL:%s", name, queueUrl));
		return queueUrl;
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

		GetQueueAttributesRequest getQueueArnRequest = new GetQueueAttributesRequest(deadLetterQueueUrl).withAttributeNames("QueueArn");
		GetQueueAttributesResult getQueueArnResult = client.getQueueAttributes(getQueueArnRequest);
		
		String deadLetterQueueArn = getQueueArnResult.getAttributes().get("QueueArn");
		
		//Set dead letter queue with redrive policy on source queue.
		String queueUrl = client.getQueueUrl(queueName).getQueueUrl();
		
		SetQueueAttributesRequest request = new SetQueueAttributesRequest()
		.withQueueUrl(queueUrl)
		.addAttributesEntry("RedrivePolicy", "{\"maxReceiveCount\":\"2\", \"deadLetterTargetArn\":\"" + deadLetterQueueArn + "\"}");
		
		client.setQueueAttributes(request);
		
		System.out.println(String.format("Queue:%s DeadLetterQueue=5s", queueName, deadLetterQueueName));

	}
	
	public void queueDelete(String queueUrl) {
		
		client.deleteQueue(queueUrl);

		System.out.println(String.format("Queue:%s Deleted.", queueUrl));
		
	}
	
	////
	
	public void messageReceive(String queueUrl, int visibilitySeconds) {
		
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
		    client.changeMessageVisibility(queueUrl, m.getReceiptHandle(), visibilitySeconds);
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
	
	public void messageSend(String queueUrl, String data) {
		
		SendMessageRequest request = new SendMessageRequest()
		        .withQueueUrl(queueUrl)
		        .withMessageBody(data)
		        //.withDelaySeconds(5)
		        ;
		SendMessageResult result = client.sendMessage(request);
		
		System.out.println(String.format("Queue:%s Set Message: %s", queueUrl, result.getMessageId()));
		
	}
	
	
	
}
