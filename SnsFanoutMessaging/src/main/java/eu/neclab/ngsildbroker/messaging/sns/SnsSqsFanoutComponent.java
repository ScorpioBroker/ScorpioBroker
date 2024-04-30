package eu.neclab.ngsildbroker.messaging.sns;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.CamelContext;
import org.apache.camel.component.aws2.sns.Sns2Component;
import org.apache.camel.component.aws2.sns.Sns2Endpoint;
import org.apache.camel.component.aws2.sqs.Sqs2Component;
import org.apache.camel.component.aws2.sqs.Sqs2Endpoint;
import org.apache.camel.spi.annotations.Component;
import org.apache.camel.support.DefaultComponent;
import org.eclipse.microprofile.config.ConfigProvider;

import java.util.Map;

@Component("sns-fanout")
@ApplicationScoped
public class SnsSqsFanoutComponent extends DefaultComponent {

    private Sns2Component snsComponent;
    private Sqs2Component sqsComponent;

    public SnsSqsFanoutComponent() {
        this(null);
    }

    public SnsSqsFanoutComponent(CamelContext context) {
        this(context, new Sns2Component(context), new Sqs2Component((context)));
    }

    public SnsSqsFanoutComponent(CamelContext context, Sns2Component snsComponent, Sqs2Component sqsComponent) {
        super(context);
        this.snsComponent = snsComponent;
        this.snsComponent.getConfiguration().setAutoCreateTopic(true);
        this.snsComponent.getConfiguration().setUseDefaultCredentialsProvider(true);

        this.sqsComponent = sqsComponent;
        this.sqsComponent.getConfiguration().setAutoCreateQueue(true);
        this.sqsComponent.getConfiguration().setUseDefaultCredentialsProvider(true);
    }

    @Override
    protected SnsSqsFanoutEndpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        initCamelContext();

        String appName = ConfigProvider.getConfig().getValue("quarkus.application.name", String.class);
        String qualifiedApplication = remaining + "-" + appName;

        Sns2Endpoint snsEndpoint = (Sns2Endpoint) this.snsComponent.createEndpoint(String.format("aws2-sns://%s", remaining), Map.of());
        Sqs2Endpoint sqsEndpoint = (Sqs2Endpoint) this.sqsComponent.createEndpoint(String.format("aws2-sqs://%s", qualifiedApplication), parameters);

        return new SnsSqsFanoutEndpoint(uri, this, snsEndpoint, sqsEndpoint);
    }

    private void initCamelContext() {
        this.snsComponent.setCamelContext(getCamelContext());
        this.sqsComponent.setCamelContext(getCamelContext());
    }


}
