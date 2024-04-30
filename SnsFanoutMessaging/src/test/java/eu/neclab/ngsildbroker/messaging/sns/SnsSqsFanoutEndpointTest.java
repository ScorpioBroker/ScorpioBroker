package eu.neclab.ngsildbroker.messaging.sns;

import org.apache.camel.CamelContext;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.component.aws2.sns.Sns2Configuration;
import org.apache.camel.component.aws2.sns.Sns2Endpoint;
import org.apache.camel.component.aws2.sqs.Sqs2Configuration;
import org.apache.camel.component.aws2.sqs.Sqs2Endpoint;
import org.apache.camel.support.DefaultComponent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class SnsSqsFanoutEndpointTest {

    @Mock
    Sns2Endpoint snsEndpoint;

    @Mock
    Sqs2Endpoint sqsEndpoint;

    @Mock
    DefaultComponent defaultComponent;

    @Mock
    Sns2Configuration sns2Configuration;

    @Mock
    Sqs2Configuration sqs2Configuration;

    @Mock
    SnsClient snsClient;

    @Mock
    SqsClient sqsClient;


    private SnsSqsFanoutEndpoint endpoint;

    private String uri = "mock-uri";
    private String queueArn = "queue-arn";
    private String topicArn = "queue-arn";
    private AutoCloseable mockContext;

    @BeforeEach
    void setUp() {
        mockContext = MockitoAnnotations.openMocks(this);

        when(snsEndpoint.getSNSClient()).thenReturn(snsClient);
        when(snsEndpoint.getConfiguration()).thenReturn(sns2Configuration);
        when(sns2Configuration.getTopicArn()).thenReturn(topicArn);

        when(sqsEndpoint.getClient()).thenReturn(sqsClient);
        when(sqsEndpoint.getConfiguration()).thenReturn(sqs2Configuration);

        when(sqsClient.getQueueUrl(any(GetQueueUrlRequest.class))).thenReturn(GetQueueUrlResponse.builder().queueUrl("queue-url").build());
        when(sqsClient.getQueueAttributes(any(GetQueueAttributesRequest.class))).thenReturn(GetQueueAttributesResponse.builder().attributes(Map.of(QueueAttributeName.QUEUE_ARN, queueArn)).build());

        endpoint = new SnsSqsFanoutEndpoint(uri, defaultComponent, snsEndpoint, sqsEndpoint);
    }

    @AfterEach
    void tearDown() throws Exception {
        mockContext.close();
    }

    @Test
    void createProducer_createsSnsProducer() throws Exception {
        Producer expected = mock(Producer.class);

        when(snsEndpoint.createProducer()).thenReturn(expected);

        Producer result = endpoint.createProducer();

        assertEquals(expected, result);
    }

    @Test
    void createConsumer_createsSqsConsumer() throws Exception {
        Consumer expected = mock(Consumer.class);
        Processor processor = mock(Processor.class);

        when(sqsEndpoint.createConsumer(eq(processor))).thenReturn(expected);

        Consumer result = endpoint.createConsumer(processor);

        assertEquals(expected, result);
    }

    @Test
    void createConsumer_subscribesToSns() throws Exception {
        endpoint.createConsumer(mock(Processor.class));

        verify(snsClient, times(1)).subscribe(eq(SubscribeRequest.builder()
                .protocol("sqs")
                .topicArn(topicArn)
                .endpoint(queueArn)
                .build()));
    }

    @Test
    void doInit_delegatesToEndpoints() throws Exception {
        endpoint.setCamelContext(mock(CamelContext.class));

        endpoint.doInit();

        verify(snsEndpoint, times(1)).init();
        verify(sqsEndpoint, times(1)).init();
    }

    @Test
    void doStart_delegatesToEndpoints() throws Exception {
        endpoint.doStart();

        verify(snsEndpoint, times(1)).start();
        verify(sqsEndpoint, times(1)).start();
    }

    @Test
    void doBuild_delegatesToEndpoints() throws Exception {
        endpoint.doBuild();

        verify(snsEndpoint, times(1)).build();
        verify(sqsEndpoint, times(1)).build();
    }

    @Test
    void doSuspend_delegatesToEndpoints() throws Exception {
        endpoint.doSuspend();

        verify(snsEndpoint, times(1)).suspend();
        verify(sqsEndpoint, times(1)).suspend();
    }

    @Test
    void doResume_delegatesToEndpoints() throws Exception {
        endpoint.doResume();

        verify(snsEndpoint, times(1)).resume();
        verify(sqsEndpoint, times(1)).resume();
    }

    @Test
    void doShutdown_delegatesToEndpoints() throws Exception {
        endpoint.doShutdown();

        verify(snsEndpoint, times(1)).shutdown();
        verify(sqsEndpoint, times(1)).shutdown();
    }

    @Test
    void doStop_delegatesToEndpoints() throws Exception {
        endpoint.doStop();

        verify(snsEndpoint, times(1)).stop();
        verify(sqsEndpoint, times(1)).stop();
    }

    @Test
    void configureProperties_configuresSqsQueue() {
        Map<String, Object> config = Map.of("Config-A", "A");
        endpoint.configureProperties(config);

        verify(sqsEndpoint, times(1)).configureProperties(eq(config));
    }

    @Test
    void setCamelContext_delegatesToEndpoints() {
        CamelContext camelContext = mock(CamelContext.class);

        endpoint.setCamelContext(camelContext);

        verify(sqsEndpoint, times(1)).setCamelContext(eq(camelContext));
        verify(snsEndpoint, times(1)).setCamelContext(eq(camelContext));
    }

}