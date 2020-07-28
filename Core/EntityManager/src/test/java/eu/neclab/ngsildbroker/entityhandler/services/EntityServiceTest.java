package eu.neclab.ngsildbroker.entityhandler.services;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.EntityDetails;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.entityhandler.config.EntityProducerChannel;
import eu.neclab.ngsildbroker.entityhandler.config.EntityTopicMap;

//@RunWith(SpringRunner.class)
//@SpringBootTest
//Set powemock runner
@RunWith(PowerMockRunner.class)
// Delegate to Spring
@PowerMockRunnerDelegate(SpringRunner.class)
@PowerMockIgnore({ "javax.management.*" })
public class EntityServiceTest {

	@Mock
	KafkaOps operations;
	@MockBean
	private ObjectMapper objectMapper;
	@MockBean
	private EntityTopicMap entityTopicMap;
	@Mock
	EntityProducerChannel entityProducerChannel;
	@InjectMocks
	@Spy
	private EntityService entityService;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	String payload;
	String updatePayload;
	String appendPayload;
	JsonNode updateJsonNode;
	JsonNode appendJsonNode;
	JsonNode blankNode;
	JsonNode payloadNode;
	

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		ReflectionTestUtils.setField(entityService, "appendOverwriteFlag", "noOverwrite");
		
		
		ObjectMapper objectMapper=new ObjectMapper();
		
		//@formatter:off
		 payload="  {\r\n" + 
				"    \"http://example.org/vehicle/brandName\": [\r\n" + 
				"      {\r\n" + 
				"        \"@value\": \"Volvo\"\r\n" + 
				"      }\r\n" + 
				"    ],\r\n" + 
				"    \"@id\": \"urn:ngsi-ld:Vehicle:B9211\",\r\n" + 
				"    \"http://example.org/vehicle/speed\": [\r\n" + 
				"      {\r\n" + 
				"        \"https://uri.etsi.org/ngsi-ld/instanceId\": [\r\n" + 
				"          {\r\n" + 
				"            \"@value\": \"be664aaf-a7af-4a99-bebc-e89528238abf\"\r\n" + 
				"          }\r\n" + 
				"        ],\r\n" + 
				"        \"https://uri.etsi.org/ngsi-ld/observedAt\": [\r\n" + 
				"          {\r\n" + 
				"            \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n" + 
				"            \"@value\": \"2018-06-01T12:03:00Z\"\r\n" + 
				"          }\r\n" + 
				"        ],\r\n" + 
				"        \"@type\": [\r\n" + 
				"          \"https://uri.etsi.org/ngsi-ld/Property\"\r\n" + 
				"        ],\r\n" + 
				"        \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + 
				"          {\r\n" + 
				"            \"@value\": \"120\"\r\n" + 
				"          }\r\n" + 
				"        ]\r\n" + 
				"      },\r\n" + 
				"      {\r\n" + 
				"        \"https://uri.etsi.org/ngsi-ld/instanceId\": [\r\n" + 
				"          {\r\n" + 
				"            \"@value\": \"d3ac28df-977f-4151-a432-dc088f7400d7\"\r\n" + 
				"          }\r\n" + 
				"        ],\r\n" + 
				"        \"https://uri.etsi.org/ngsi-ld/observedAt\": [\r\n" + 
				"          {\r\n" + 
				"            \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n" + 
				"            \"@value\": \"2018-08-01T12:05:00Z\"\r\n" + 
				"          }\r\n" + 
				"        ],\r\n" + 
				"        \"@type\": [\r\n" + 
				"          \"https://uri.etsi.org/ngsi-ld/Property\"\r\n" + 
				"        ],\r\n" + 
				"        \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + 
				"          {\r\n" + 
				"            \"@value\": \"80\"\r\n" + 
				"          }\r\n" + 
				"        ]\r\n" + 
				"      }\r\n" + 
				"    ],\r\n" + 
				"    \"@type\": [\r\n" + 
				"      \"http://example.org/vehicle/Vehicle\"\r\n" + 
				"    ]\r\n" + 
				"  }\r\n";
		
		 updatePayload="{\r\n" + 
				"	\"http://example.org/vehicle/speed\": [{\r\n" + 
				"		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + 
				"			\"@value\": \"240\"\r\n" + 
				"		}],\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/observedAt\": [{\r\n" + 
				"			\"@value\": \"2018-06-01T12:03:00\",\r\n" + 
				"			\"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\"\r\n" + 
				"		}]\r\n" + 
				"	}]\r\n" + 
				"}";
		 
		 appendPayload="{\r\n" + 
					"	\"http://example.org/vehicle/speed1\": [{\r\n" + 
					"		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n" + 
					"		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + 
					"			\"@value\": \"240\"\r\n" + 
					"		}],\r\n" + 
					"		\"https://uri.etsi.org/ngsi-ld/observedAt\": [{\r\n" + 
					"			\"@value\": \"2018-06-01T12:03:00\",\r\n" + 
					"			\"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\"\r\n" + 
					"		}]\r\n" + 
					"	}]\r\n" + 
					"}";
		 
		 
		//@formatter:on
		 
		 updateJsonNode=objectMapper.readTree(updatePayload);
		 appendJsonNode=objectMapper.readTree(appendPayload);
		 blankNode=objectMapper.createObjectNode();
		 payloadNode=objectMapper.readTree(payload);
	}

	@After
	public void tearDown() {
		payload = null;
		updatePayload = null;
		appendPayload=null;
	}

	@Test
	public void createMessageTest(){
		try {
			JsonNode jsonNode = Mockito.mock(JsonNode.class);
			Mockito.doReturn(jsonNode).when(objectMapper).readTree(payload);
			Mockito.doReturn(payload).when(objectMapper).writeValueAsString(any());
			Mockito.doReturn(jsonNode).when(jsonNode).get(any());
			Mockito.doReturn(false).when(entityTopicMap).isExist(any());
			Mockito.doReturn("urn:ngsi-ld:Vehicle:B9211").when(jsonNode).asText();
			Mockito.doReturn(true).when(entityService).registerContext(any(), any());
			Mockito.doReturn(true).when(operations).pushToKafka(any(), any(), any());
			Mockito.doReturn(jsonNode).when(entityService).getKeyValueEntity(jsonNode);
	
			String id = entityService.createMessage(payload);
			Assert.assertEquals(id, "urn:ngsi-ld:Vehicle:B9211");
	
			verify(entityTopicMap, times(1)).isExist(any());
			verify(entityService, times(1)).registerContext(any(), any());
			verify(operations, times(4)).pushToKafka(any(), any(), any());
		}catch( Exception ex) {
			Assert.fail();
		}
		
	}

	@Test
	public void createMessageThrowsAlreadyExistTest() throws ResponseException, Exception {
		thrown.expect(ResponseException.class);
		thrown.expectMessage("Already exists.");

		JsonNode jsonNode = Mockito.mock(JsonNode.class);
		Mockito.doReturn(jsonNode).when(objectMapper).readTree(payload);
		Mockito.doReturn(jsonNode).when(jsonNode).get(any());
		Mockito.doReturn(true).when(entityTopicMap).isExist(any());
		Mockito.doReturn("urn:ngsi-ld:Vehicle:B9211").when(jsonNode).asText();
		Mockito.doReturn(true).when(entityService).registerContext(any(), any());
		Mockito.doReturn(true).when(operations).pushToKafka(any(), any(), any());

		entityService.createMessage(payload);

		verify(entityTopicMap, times(1)).isExist(any());
		
	}
	
//	@Test
	public void updateMessageTest(){
		try {
			EntityDetails entityDetails=Mockito.mock(EntityDetails.class);
			byte[] messageByte=payload.getBytes();
			JsonNode resultJson = objectMapper.createObjectNode();
			UpdateResult updateResult = new UpdateResult(updateJsonNode, resultJson);
			updateResult.setStatus(true);
			
			Mockito.doReturn(entityDetails).when(entityTopicMap).get(any());
			Mockito.doReturn(messageByte).when(operations).getMessage(any(), any(), any(Integer.class), any(Long.class));
			Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(String.class));
			Mockito.doReturn(blankNode).when(objectMapper).createObjectNode();
//			Mockito.doReturn(updateResult).when(entityService).updateFields(messageByte, updateJsonNode, null);
			//TODO no assert. no usage of result 
			UpdateResult result=entityService.updateMessage("urn:ngsi-ld:Vehicle:B9211", updatePayload);
		}catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
		
	}
	
	@Test
	public void updateFieldTest() {
		try {
			Mockito.doReturn(blankNode).when(objectMapper).createObjectNode();
			Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(String.class));
			
			UpdateResult updateResult=entityService.updateFields(payload,updateJsonNode , null);
			
			Assert.assertTrue(updateResult.getStatus());
			Assert.assertEquals(updateJsonNode, updateResult.getJsonToAppend());
		}catch (Exception ex) {
			Assert.fail();
		}
	}
	
	@Test
	public void appendFieldTest(){
		try {
			Mockito.doReturn(blankNode).when(objectMapper).createObjectNode();
			Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(String.class));
			AppendResult appendResult=entityService.appendFields(payload, updateJsonNode, " ");
			Assert.assertTrue(appendResult.getStatus());
		}catch(Exception ex) {
			Assert.fail();
		}
	}
	
	@Test
	public void deleteField404Test() throws ResponseException, Exception {
		thrown.expect(ResponseException.class);
		thrown.expectMessage("Resource not found.");
		
		Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(String.class));
		entityService.deleteFields(payload, "notPresent");
	}
	
	@Test
	public void deleteFieldTest() {
		try {
			Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(String.class));
			entityService.deleteFields(payload, "http://example.org/vehicle/speed");
		}catch(Exception ex) {
			Assert.fail();
		}
	}
	
	
}
