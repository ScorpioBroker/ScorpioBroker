package org.acme;

import org.apache.kafka.clients.admin.NewTopic;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.springframework.kafka.core.KafkaAdmin;

@Liveness
public class MyLivenessCheck implements HealthCheck {

    @Override
    public HealthCheckResponse call() {
    	KafkaAdmin admin = new KafkaAdmin(null);
    	admin.setAutoCreate(true);
    	admin.createOrModifyTopics(new NewTopic(null, null, null));
    	
        return HealthCheckResponse.up("alive");
        
    }

}