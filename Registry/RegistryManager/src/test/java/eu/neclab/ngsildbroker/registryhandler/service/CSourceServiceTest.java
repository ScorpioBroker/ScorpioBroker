package eu.neclab.ngsildbroker.registryhandler.service;

import static org.mockito.ArgumentMatchers.any;

import java.io.IOException;
import java.net.URI;

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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.registryhandler.config.CSourceProducerChannel;

@SpringBootTest
@RunWith(SpringRunner.class)
public class CSourceServiceTest {

	

	@Mock
	KafkaOps operations;
	@Mock
	CSourceProducerChannel producerChannels;
	@InjectMocks
	CSourceService csourceService;
	@MockBean
	ObjectMapper objectMapper;
	@Mock
	CSourceSubscriptionService csourceSubService;
	

	private CSourceRegistration csourceReg;
	private CSourceRegistration updateCSourceReg;
	String payload;
	String updatePayload;

	JsonNode blankNode;
	JsonNode payloadNode;

	@Before
	public void setup() throws IOException {
		MockitoAnnotations.initMocks(this);
		ObjectMapper objectMapper = new ObjectMapper();
		//@formatter:off
		payload="{\r\n" + 
				"	\"https://uri.etsi.org/ngsi-ld/endpoint\": [{\r\n" + 
				"		\"@value\": \"http://my.csource.org:1026\"\r\n" + 
				"	}],\r\n" + 
				"	\"@id\": \"urn:ngsi-ld:ContextSourceRegistration:csr1a3456\",\r\n" + 
				"	\"https://uri.etsi.org/ngsi-ld/information\": [{\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/entities\": [{\r\n" + 
				"			\"@id\": \"urn:ngsi-ld:Vehicle:A456\",\r\n" + 
				"			\"@type\": [\"https://json-ld.org/playground/Vehicle\"]\r\n" + 
				"		}],\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/properties\": [{\r\n" + 
				"			\"@id\": \"https://json-ld.org/playground/brandName\"\r\n" + 
				"		},\r\n" + 
				"		{\r\n" + 
				"			\"@id\": \"https://json-ld.org/playground/speed\"\r\n" + 
				"		}],\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/relationships\": [{\r\n" + 
				"			\"@id\": \"https://json-ld.org/playground/isParked\"\r\n" + 
				"		}]\r\n" + 
				"	}],\r\n" + 
				"	\"https://uri.etsi.org/ngsi-ld/location\": [{\r\n" + 
				"		\"@value\": \"{ \\\"type\\\":\\\"Polygon\\\", \\\"coordinates\\\": [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],[100.0, 1.0], [100.0, 0.0] ] ]}\"\r\n" + 
				"	}],\r\n" + 
				"	\"@type\": [\"https://uri.etsi.org/ngsi-ld/ContextSourceRegistration\"]\r\n" + 
				"}";
		
		updatePayload="{\r\n" + 
				"	\"https://uri.etsi.org/ngsi-ld/endpoint\": [{\r\n" + 
				"		\"@value\": \"http://my.csource.org:1026\"\r\n" + 
				"	}],\r\n" + 
				"	\"@id\": \"urn:ngsi-ld:ContextSourceRegistration:csr1a3456\",\r\n" + 
				"	\"https://uri.etsi.org/ngsi-ld/information\": [{\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/entities\": [{\r\n" + 
				"			\"@id\": \"urn:ngsi-ld:Vehicle:A456\",\r\n" + 
				"			\"@type\": [\"https://json-ld.org/playground/Vehicle\"]\r\n" + 
				"		}],\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/properties\": [{\r\n" + 
				"			\"@id\": \"https://json-ld.org/playground/brandName\"\r\n" + 
				"		},\r\n" + 
				"		{\r\n" + 
				"			\"@id\": \"https://json-ld.org/playground/speed\"\r\n" + 
				"		}],\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/relationships\": [{\r\n" + 
				"			\"@id\": \"https://json-ld.org/playground/isParked\"\r\n" + 
				"		}]\r\n" + 
				"	}],\r\n" + 
				"	\"https://uri.etsi.org/ngsi-ld/location\": [{\r\n" + 
				"		\"@value\": \"{ \\\"type\\\":\\\"Polygon\\\", \\\"coordinates\\\": [ [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],[100.0, 1.0], [100.0, 0.0] ] ]}\"\r\n" + 
				"	}],\r\n" + 
				"	\"@type\": [\"https://uri.etsi.org/ngsi-ld/ContextSourceRegistration\"]\r\n" + 
				"}";
		//@formatter:on
		csourceReg = DataSerializer.getCSourceRegistration(payload);
		updateCSourceReg = DataSerializer.getCSourceRegistration(updatePayload);

		payloadNode = objectMapper.readTree(payload.getBytes());
	}

	@After
	public void teardown() {
		payload = null;
		updatePayload = null;
		csourceReg = null;
		updateCSourceReg = null;
		blankNode = null;
	}

	@Test
	public void registerCSourceTest(){
		try {
			csourceReg.setInternal(true);
			Mockito.doReturn(false).when(operations).isMessageExists(any(), any());
			URI uri = csourceService.registerCSource(csourceReg);
			Assert.assertEquals(uri, new URI("urn:ngsi-ld:ContextSourceRegistration:csr1a3456"));
		}catch(Exception ex) {
			Assert.fail();
		}
	}

	@Test
	public void updateCSourceTest() {
		try {
			byte[] payloadBytes = payload.getBytes();
	
			updateCSourceReg.setInternal(false);
			Mockito.doReturn(payloadBytes).when(operations).getMessage(any(), any());
			Mockito.doReturn(blankNode).when(objectMapper).createObjectNode();
			Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(byte[].class));
			Mockito.doReturn(true).when(csourceSubService).checkSubscriptions(any(CSourceRegistration.class),
					any(CSourceRegistration.class));
	
			boolean result = csourceService.updateCSourceRegistration("urn:ngsi-ld:ContextSourceRegistration:csr1a3456",
					updatePayload);
	
			Assert.assertTrue(result);
		}catch(Exception ex) {
			Assert.fail();
		}
	}

	@Test
	public void deleteCSorceTest() throws Exception {
		try {
			byte[] payloadBytes = payload.getBytes();
			Mockito.doReturn(payloadBytes).when(operations).getMessage(any(), any());
			Mockito.doReturn(csourceReg).when(objectMapper).readValue(any(byte[].class),
					Mockito.eq(CSourceRegistration.class));
	
			boolean result = csourceService.deleteCSourceRegistration("urn:ngsi-ld:ContextSourceRegistration:csr1a3456");
	
			Assert.assertTrue(result);
		}catch(Exception ex) {
			Assert.fail();
		}
	}

}
