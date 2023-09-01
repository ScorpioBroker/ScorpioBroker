package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;

@Singleton
@UnlessBuildProfile("in-memory")
public class SubscriptionMessaging extends SubscriptionMessagingBase {

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleEntity(Object byteMessage) {
		if(byteMessage instanceof byte[] bytes) {
			byteMessage = new String(bytes);
		}
		return handleEntityRaw((String) byteMessage);
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_NOTIFICATION_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleInternalNotification(Object byteMessage) {
		if(byteMessage instanceof byte[] bytes) {
			byteMessage = new String(bytes);
		}
		return handleInternalNotificationRaw((String) byteMessage);
	}

	@Incoming(AppConstants.ENTITY_BATCH_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleBatchEntities(Object byteMessage) {
		if(byteMessage instanceof byte[] bytes) {
			byteMessage = new String(bytes);
		}
		return handleBatchEntitiesRaw((String) byteMessage);
	}

}
