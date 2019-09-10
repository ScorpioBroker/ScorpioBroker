package eu.neclab.ngsildbroker.registryhandler.service;

import static org.mockito.ArgumentMatchers.any;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.registryhandler.config.CSourceProducerChannel;

@SpringBootTest
@RunWith(SpringRunner.class)
public class CSourceSubscriptionServiceTest {
	
	@Mock
	KafkaOps operations;
	@Mock
	CSourceProducerChannel producerChannels;
	@InjectMocks
	CSourceSubscriptionService csourceSubs;
	
	String subsPayload;
	Subscription subs;

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
		subsPayload="{\r\n" + 
				"	\"https://uri.etsi.org/ngsi-ld/entities\": [{\r\n" + 
				"		\"@type\": [\"http://example.org/vehicle/Vehicle\"]\r\n" + 
				"	}],\r\n" + 
				"	\"@id\": \"urn:ngsi-ld:Subscription:7\",\r\n" + 
				"	\"https://uri.etsi.org/ngsi-ld/notification\": [{\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/attributes\": [{\r\n" + 
				"			\"@id\": \"http://example.org/vehicle/brandName\"\r\n" + 
				"		}],\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/endpoint\": [{\r\n" + 
				"			\"https://uri.etsi.org/ngsi-ld/accept\": [{\r\n" + 
				"				\"@value\": \"application/json\"\r\n" + 
				"			}],\r\n" + 
				"			\"https://uri.etsi.org/ngsi-ld/uri\": [{\r\n" + 
				"				\"@value\": \"http://my.endpoint.org/notify\"\r\n" + 
				"			}]\r\n" + 
				"		}],\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/format\": [{\r\n" + 
				"			\"@value\": \"keyValues\"\r\n" + 
				"		}]\r\n" + 
				"	}],\r\n" + 
				"	\"https://uri.etsi.org/ngsi-ld/q\": [{\r\n" + 
				"		\"@value\": \"http://example.org/vehicle/brandName!=Mercedes\"\r\n" + 
				"	}],\r\n" + 
				"	\"@type\": [\"https://uri.etsi.org/ngsi-ld/Subscription\"],\r\n" + 
				"	\"https://uri.etsi.org/ngsi-ld/watchedAttributes\": [{\r\n" + 
				"		\"@id\": \"http://example.org/vehicle/brandName\"\r\n" + 
				"	}]\r\n" + 
				"}";
		
		subs=DataSerializer.getSubscription(subsPayload);
	}
	
	@After
	public void teardown() {
		subsPayload=null;
		subs=null;
	}
	
	@Test
	public void subscribeTest() {
		try {
			Mockito.doReturn(true).when(operations).isMessageExists(any(), any());
			URI uri=csourceSubs.subscribe(new SubscriptionRequest(subs, null));
			
			Assert.assertEquals(uri, new URI("urn:ngsi-ld:Subscription:7"));
		}catch(Exception ex) {
			Assert.fail();
		}
	}
	
	
	@Test
	public void unSubscribeTest() throws Exception {
		try {
			csourceSubs.subscribe(new SubscriptionRequest(subs, null));
			Assert.assertTrue(csourceSubs.unsubscribe(new URI("urn:ngsi-ld:Subscription:7")));
		}catch(Exception ex) {
			Assert.fail();
		}
	}
	
	@Test
	public void updateSubTest() throws Exception {
		try {
			csourceSubs.subscribe(new SubscriptionRequest(subs, null));
			Subscription  newSub=subs;
			List<String> watchedAttrib=new ArrayList<>();	
			watchedAttrib.add("http://example.org/vehicle/brandName2");
			newSub.setAttributeNames(watchedAttrib);
			
			Subscription updatedSub=csourceSubs.updateSubscription(newSub);
			
			Assert.assertEquals("http://example.org/vehicle/brandName2", updatedSub.getAttributeNames().get(0));
		}catch(Exception ex) {
			Assert.fail();
		}
	}
}
