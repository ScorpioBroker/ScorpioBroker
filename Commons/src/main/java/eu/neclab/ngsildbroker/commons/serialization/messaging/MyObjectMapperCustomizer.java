package eu.neclab.ngsildbroker.commons.serialization.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.quarkus.jackson.ObjectMapperCustomizer;
import io.quarkus.runtime.StartupEvent;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Singleton;

@Singleton
public class MyObjectMapperCustomizer implements ObjectMapperCustomizer {

	void startup(@Observes StartupEvent event) {
	}
	
	@Override
	public void customize(ObjectMapper objectMapper) {
		SimpleModule tmp = new SimpleModule();
		tmp.addDeserializer(BaseRequest.class, new BaseRequestDeserializer());
		objectMapper.registerModule(tmp);

	}

}
