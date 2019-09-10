package eu.neclab.ngsildbroker.registryhandler.service;

import java.io.IOException;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceNotification;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.interfaces.CSourceNotificationHandler;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

@Service
public class CSourceNotificationHandlerREST implements CSourceNotificationHandler{
	
	private final static Logger logger = LogManager.getLogger(CSourceNotificationHandlerREST.class);
	
	

	private ContextResolverBasic contextResolver;
	HttpUtils httpUtils;
	
	public CSourceNotificationHandlerREST(ContextResolverBasic contextResolver) {
		this.contextResolver = contextResolver;
		httpUtils = HttpUtils.getInstance(contextResolver);
	}

	@Override
	public void notify(CSourceNotification notification, Subscription sub) {
		String regString = DataSerializer.toJson(notification);
		//TODO rework when storage of sub context is done
		//regString = contextResolver.simplify(regString, contextResolver.getContextAsSet(sub.getId().toString()), true).getSimplifiedCompletePayload();
		HashMap<String, String> addHeaders = new HashMap<String, String>();
		if(sub.getNotification().getEndPoint().getAccept() != null) {
			addHeaders.put("accept", sub.getNotification().getEndPoint().getAccept());
		}
		try {
			httpUtils.doPost(sub.getNotification().getEndPoint().getUri(), regString, addHeaders);
		} catch (IOException e) {
			logger.error("Failed to send notification to endpoint " + sub.getNotification().getEndPoint().getUri());
		}
		
		
	}
	
	
	


	

}
