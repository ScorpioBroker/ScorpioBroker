package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.net.URI;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

public class NotificationHandlerREST extends BaseNotificationHandler {

	
	
	public NotificationHandlerREST(SubscriptionService subscriptionManagerService,
			ObjectMapper objectMapper) {
		super(subscriptionManagerService, objectMapper);
		
	}

	@Override
	protected void sendReply(ResponseEntity<byte[]> reply, URI callback, Map<String, String> clientSettings) throws Exception {
		HttpUtils.doPost(callback, reply.getBody(),
				reply.getHeaders().toSingleValueMap());
		
	}

}
