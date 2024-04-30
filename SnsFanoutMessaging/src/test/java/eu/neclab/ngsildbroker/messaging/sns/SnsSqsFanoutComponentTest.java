package eu.neclab.ngsildbroker.messaging.sns;

import org.apache.camel.CamelContext;
import org.apache.camel.component.aws2.sns.Sns2Component;
import org.apache.camel.component.aws2.sns.Sns2Configuration;
import org.apache.camel.component.aws2.sns.Sns2Endpoint;
import org.apache.camel.component.aws2.sqs.Sqs2Component;
import org.apache.camel.component.aws2.sqs.Sqs2Configuration;
import org.apache.camel.component.aws2.sqs.Sqs2Endpoint;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class SnsSqsFanoutComponentTest {

    @Mock
    Sns2Component snsComponent;

    @Mock
    Sqs2Component sqs2Component;

    @Mock
    CamelContext camelContext;

    @Mock
    Sns2Configuration sns2Configuration;

    @Mock
    Sqs2Configuration sqs2Configuration;

    @Mock
    SnsClient snsClient;

    @Mock
    SqsClient sqsClient;


    private SnsSqsFanoutComponent component;

    private AutoCloseable mockContext;
    private MockedStatic<ConfigProvider> configProviderMock;
    private String appName = "test-app";

    @BeforeEach
    void setUp() {
        mockContext = MockitoAnnotations.openMocks(this);
        configProviderMock = mockStatic(ConfigProvider.class);

        Config config = mock(Config.class);
        when(config.getValue(eq("quarkus.application.name"), eq(String.class))).thenReturn(appName);
        configProviderMock.when(ConfigProvider::getConfig).thenReturn(config);

        when(snsComponent.getConfiguration()).thenReturn(sns2Configuration);
        when(sqs2Component.getConfiguration()).thenReturn(sqs2Configuration);

        component = new SnsSqsFanoutComponent(camelContext, snsComponent, sqs2Component);
    }

    @AfterEach
    void tearDown() throws Exception {
        mockContext.close();
        configProviderMock.close();
    }

    @Test
    void createEndpoint_configuresQueueAndTopic() {
        verify(sns2Configuration, times(1)).setAutoCreateTopic(eq(true));
        verify(sns2Configuration, times(1)).setUseDefaultCredentialsProvider(eq(true));

        verify(sqs2Configuration, times(1)).setAutoCreateQueue(eq(true));
        verify(sqs2Configuration, times(1)).setUseDefaultCredentialsProvider(eq(true));
    }

    @Test
    void createEndpoint_initializesSnsComponent() throws Exception {
        String uri = "my-uri";
        String remaining = "PROCESS";
        Map<String, Object> parameters = Map.of("ParamA", "A");
        Sns2Endpoint targetEndpoint = mock(Sns2Endpoint.class);

        when(snsComponent.createEndpoint(any(), any())).thenReturn(targetEndpoint);

        SnsSqsFanoutEndpoint endpoint = component.createEndpoint(uri, remaining, parameters);

        assertEquals(endpoint.getSns2Endpoint(), targetEndpoint);
        verify(snsComponent, times(1)).createEndpoint(eq("aws2-sns://PROCESS"), eq(Map.of()));
    }

    @Test
    void createEndpoint_initializesSqsComponent() throws Exception {
        String uri = "my-uri";
        String remaining = "PROCESS";
        Map<String, Object> parameters = Map.of("ParamA", "A");
        Sqs2Endpoint targetEndpoint = mock(Sqs2Endpoint.class);

        when(sqs2Component.createEndpoint(any(), any())).thenReturn(targetEndpoint);

        SnsSqsFanoutEndpoint endpoint = component.createEndpoint(uri, remaining, parameters);

        assertEquals(endpoint.getSqs2Endpoint(), targetEndpoint);
        verify(sqs2Component, times(1)).createEndpoint(eq(String.format("aws2-sqs://%s-%s", remaining, appName)), eq(parameters));
    }

    @Test
    void createEndpoint_initializesCamelContext() throws Exception {
        component.createEndpoint("uri", "remaining", Map.of());

        verify(sqs2Component, times(1)).setCamelContext(eq(camelContext));
        verify(snsComponent, times(1)).setCamelContext(eq(camelContext));
    }
}