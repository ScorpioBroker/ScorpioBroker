package eu.neclab.ngsildbroker.messaging.sns;

import org.apache.camel.*;
import org.apache.camel.component.aws2.sns.Sns2Endpoint;
import org.apache.camel.component.aws2.sqs.Sqs2Endpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.DefaultEndpoint;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.Map;

@UriEndpoint(
        firstVersion = "3.14.4",
        scheme = "sns-fanout",
        title = "SNS Fanout Component",
        syntax = "sns-fanout:topic",
        category = {Category.CLOUD}
)
public class SnsSqsFanoutEndpoint extends DefaultEndpoint {
    private Sns2Endpoint sns2Endpoint;
    private Sqs2Endpoint sqs2Endpoint;

    public SnsSqsFanoutEndpoint(String uri, DefaultComponent component, Sns2Endpoint sns2Endpoint, Sqs2Endpoint sqs2Endpoint) {
        super(uri, component);

        this.sns2Endpoint = sns2Endpoint;
        this.sqs2Endpoint = sqs2Endpoint;
    }

    @Override
    public Producer createProducer() throws Exception {
        return sns2Endpoint.createProducer();
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        Consumer consumer = sqs2Endpoint.createConsumer(processor);
        subscribeToTopic();

        return consumer;
    }

    private void subscribeToTopic() {
        String queueArn = getQueueArn();

        sns2Endpoint.getSNSClient().subscribe(SubscribeRequest.builder()
                .protocol("sqs")
                .endpoint(queueArn)
                .topicArn(sns2Endpoint.getConfiguration().getTopicArn())
                .build());
    }

    private String getQueueArn() {
        String queueUrl = sqs2Endpoint.getClient().getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(sqs2Endpoint.getConfiguration().getQueueName())
                        .build())
                .queueUrl();

        return sqs2Endpoint.getClient().getQueueAttributes(GetQueueAttributesRequest.builder()
                        .queueUrl(queueUrl)
                        .attributeNames(QueueAttributeName.QUEUE_ARN)
                        .build()
                )
                .attributes()
                .get(QueueAttributeName.QUEUE_ARN);
    }

    @Override
    protected void doInit() throws Exception {
        super.doInit();
        this.sns2Endpoint.init();
        this.sqs2Endpoint.init();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        this.sns2Endpoint.start();
        this.sqs2Endpoint.start();
    }

    @Override
    protected void doBuild() throws Exception {
        super.doBuild();
        this.sns2Endpoint.build();
        this.sqs2Endpoint.build();
    }

    @Override
    protected void doSuspend() throws Exception {
        super.doSuspend();
        this.sns2Endpoint.suspend();
        this.sqs2Endpoint.suspend();
    }

    @Override
    protected void doResume() throws Exception {
        super.doResume();
        this.sns2Endpoint.resume();
        this.sqs2Endpoint.resume();
    }

    @Override
    protected void doShutdown() throws Exception {
        super.doShutdown();
        this.sns2Endpoint.shutdown();
        this.sqs2Endpoint.shutdown();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        this.sns2Endpoint.stop();
        this.sqs2Endpoint.stop();
    }

    @Override
    public void configureProperties(Map<String, Object> options) {
        this.sqs2Endpoint.configureProperties(options);
    }

    @Override
    public void setCamelContext(CamelContext camelContext) {
        super.setCamelContext(camelContext);
        this.sns2Endpoint.setCamelContext(camelContext);
        this.sqs2Endpoint.setCamelContext(camelContext);
    }

    public Sns2Endpoint getSns2Endpoint() {
        return sns2Endpoint;
    }


    public Sqs2Endpoint getSqs2Endpoint() {
        return sqs2Endpoint;
    }

}
