package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.net.URI;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;

public class NotificationHandlerREST extends BaseNotificationHandler {

	
	
	public NotificationHandlerREST(SubscriptionService subscriptionManagerService, ContextResolverBasic contextResolver,
			ObjectMapper objectMapper) {
		super(subscriptionManagerService, contextResolver, objectMapper);
		
	}

	@Override
	protected void sendReply(ResponseEntity<byte[]> reply, URI callback, Map<String, String> clientSettings) throws Exception {
		httpUtils.doPost(callback, reply.getBody(),
				reply.getHeaders().toSingleValueMap());
		
	}

}
