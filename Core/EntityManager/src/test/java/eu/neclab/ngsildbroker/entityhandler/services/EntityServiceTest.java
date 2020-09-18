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
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
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

	String updatePayload;
	String appendPayload;
	String entityPayload;
	String updatePartialAttributesPayload;
	String updatePartialDefaultAttributesPayload;
	JsonNode updateJsonNode;
	JsonNode appendJsonNode;
	JsonNode blankNode;
	JsonNode payloadNode;
	JsonNode updatePartialAttributesNode;
	JsonNode updatePartialDefaultAttributesNode;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		ReflectionTestUtils.setField(entityService, "appendOverwriteFlag", "noOverwrite");
		ObjectMapper objectMapper=new ObjectMapper();
		
		//@formatter:off
		
		entityPayload="{\r\n" + 
				"    \"http://example.org/vehicle/brandName\": [\r\n" + 
				"      {\r\n" + 
				"        \"@type\":[\r\n" + 
				"    \"https://uri.etsi.org/ngsi-ld/Property\"],\r\n" +
				"    \"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" +
				"     \"@value\": \"Mercedes\"\r\n" + 
				"      }]\r\n" + 
				"    }],\r\n" + 
				"        \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n" + 
				"          {\r\n" + 
				"            \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n" + 
				"            \"@value\": \"2018-06-01T12:03:00Z\"\r\n" + 
				"          }\r\n" + 
				"        ],\r\n" + 
				"     \"@id\": \"urn:ngsi-ld:Vehicle:A103\",\r\n" +
				"    \"https://uri.etsi.org/ngsi-ld/modifiedAt\":[{\r\n" +
				"     \"@value\": \"2017-07-29T12:00:04Z\",\r\n" +
				"     \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\"}],\r\n" +
				"     \"http://example.org/vehicle/speed\": [\r\n" +
				"     {\r\n" + 
				"    \"https://uri.etsi.org/ngsi-ld/datasetId\": [\r\n" +
				"     {\r\n" +
				"     \"@id\": \"urn:ngsi-ld:Property:speedometerA4567-speed\"\r\n" +
				"      }\r\n" +
				"      ],\r\n" +
				"    \"https://uri.etsi.org/ngsi-ld/default-context/source\":[\r\n" +
				"     {\r\n" + 
				"     \"@type\":[\r\n" +
				"    \"https://https://uri.etsi.org/ngsi-ld/Property\"],\r\n" +
				"    \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" +
	            "		{\r\n" +	
	            "     \"@value\": \"Speedometer\"\r\n" +
	            "      }\r\n" +
	            "      ]\r\n" +
	            "      }\r\n" +
	            "      ],\r\n" +
	            "       \"@type\":[\r\n" +
	            "    \"https://uri.etsi.org/ngsi-ld/Property\"],\r\n" +
	            "     \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" +
	            "		{\r\n" +
	            "    \"value\":55\r\n" + 
	            "      }\r\n" +
	            "        ]\r\n" +
				"      },\r\n" +
				"     {\r\n" + 
				"    \"https://uri.etsi.org/ngsi-ld/default-context/source\":[\r\n" +
				"     {\r\n" +
				"     \"@type\":[\r\n" +
				"    \"https://https://uri.etsi.org/ngsi-ld/Property\"],\r\n" +
				"    \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" +
	            "		{\r\n" +	
	            "     \"@value\": \"GPS\"\r\n" +
	            "      }\r\n" +
	            "      ]\r\n" +
				"     }\r\n" +
				"      ],\r\n" +
				"     \"@type\":[\r\n" +
				"    \"https://https://uri.etsi.org/ngsi-ld/Property\"],\r\n" +
				"     \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" +
		        "	  {\r\n" +
		        "    \"value\":10\r\n" + 
		        "      }\r\n" +
		        "     ]\r\n" +
				"     }\r\n" + 
				"     ],\r\n" + 
				"     \"@type\":[\r\n" + 
				"    \"http://example.org/vehicle/Vehicle\"]\r\n" +
				"  }\r\n";
		 
		updatePayload="{\r\n" + 
				"	\"http://example.org/vehicle/brandName\": [{\r\n" + 
				"		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + 
				"			\"@value\": \"AUDI\"\r\n" +
				"		}]\r\n" + 
				"	}]\r\n" + 
				"}";
		updatePartialAttributesPayload="{\r\n" + 
				"	\"https://uri.etsi.org/ngsi-ld/datasetId\": [{\r\n" + 
				"		\"@id\": \"urn:ngsi-ld:Property:speedometerA4567-speed\" \r\n" + 
				"	}],\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + 
				"			\"@value\": \"20\"\r\n" +
				"		}]\r\n" + 
				"}";
		updatePartialDefaultAttributesPayload="{\r\n" + 
				"		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + 
				"			\"@value\": \"20\"\r\n" +
				"		}]\r\n" + 
				"}";
		 
		 appendPayload="{\r\n" + 
					"	\"http://example.org/vehicle/brandName1\": [{\r\n" + 
					"		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n" + 
					"		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + 
					"			\"@value\": \"BMW\"\r\n" +
					"		}]\r\n" + 
					"	}]\r\n" + 
					"}";
		 
		//@formatter:on
		 
		 updateJsonNode=objectMapper.readTree(updatePayload);
		 appendJsonNode=objectMapper.readTree(appendPayload);
		 blankNode=objectMapper.createObjectNode();
		 payloadNode=objectMapper.readTree(entityPayload);
		 updatePartialAttributesNode=objectMapper.readTree(updatePartialAttributesPayload);
		 updatePartialDefaultAttributesNode=objectMapper.readTree(updatePartialDefaultAttributesPayload);
	}

	@After
	public void tearDown() {
		updatePayload = null;
		appendPayload=null;
		entityPayload=null;
		updatePartialAttributesPayload=null;
		updatePartialDefaultAttributesPayload=null;
	}

	/**
	 * this method is use for create the entity
	 */
	@Test
	public void createMessageTest(){
		try {
			JsonNode jsonNode = Mockito.mock(JsonNode.class);
			Mockito.doReturn(jsonNode).when(objectMapper).readTree(entityPayload);
			Mockito.doReturn(entityPayload).when(objectMapper).writeValueAsString(any());
			Mockito.doReturn(jsonNode).when(jsonNode).get(any());
			Mockito.doReturn(false).when(entityTopicMap).isExist(any());
			Mockito.doReturn("urn:ngsi-ld:Vehicle:A103").when(jsonNode).asText();
			Mockito.doReturn(true).when(entityService).registerContext(any(), any());
			Mockito.doReturn(true).when(operations).pushToKafka(any(), any(), any());
			Mockito.doReturn(jsonNode).when(entityService).getKeyValueEntity(jsonNode);
	
			String id = entityService.createMessage(entityPayload);
			Assert.assertEquals(id, "urn:ngsi-ld:Vehicle:A103");
			verify(entityService, times(1)).getKeyValueEntity(any());
		}catch( Exception ex) {
			Assert.fail();
		}
		
	}

	/**
	 * this method is validate the entity if already exist
	 * @throws ResponseException
	 * @throws Exception
	 */
	@Test
	public void createMessageThrowsAlreadyExistTest() throws ResponseException, Exception {
		thrown.expect(ResponseException.class);
		thrown.expectMessage("Already exists.");
		JsonNode jsonNode = Mockito.mock(JsonNode.class);
		Mockito.doReturn(jsonNode).when(objectMapper).readTree(entityPayload);
		Mockito.doReturn(jsonNode).when(jsonNode).get(any());
		Mockito.doReturn(true).when(entityTopicMap).isExist(any());
		Mockito.doReturn("urn:ngsi-ld:Vehicle:A103").when(jsonNode).asText();
		Mockito.doReturn(true).when(entityService).registerContext(any(), any());
		Mockito.doReturn(true).when(operations).pushToKafka(any(), any(), any());
		Mockito.doThrow(new ResponseException(ErrorType.AlreadyExists)).when(entityService).createMessage(any());
		entityService.createMessage(entityPayload);
		verify(entityTopicMap, times(1)).isExist(any());
		
	}
	
	/**
	 * this method is use for update the entity
	 * @throws Exception 
	 */
	@Test
	public void updateMessageTest() throws Exception{
		
			EntityDetails entityDetails=Mockito.mock(EntityDetails.class);
			byte[] messageByte=entityPayload.getBytes();
			JsonNode resultJson = objectMapper.createObjectNode();
			UpdateResult updateResult = new UpdateResult(updateJsonNode, resultJson);
			updateResult.setStatus(true);
			
			Mockito.doReturn(entityDetails).when(entityTopicMap).get(any());
			Mockito.doReturn(messageByte).when(operations).getMessage(any(), any(), any(Integer.class), any(Long.class));
			Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(String.class));
			Mockito.doReturn(blankNode).when(objectMapper).createObjectNode();
//			Mockito.doReturn(updateResult).when(entityService).updateFields(messageByte, updateJsonNode, null);
			//TODO no assert. no usage of result 
			Mockito.doReturn(updateResult).when(entityService).updateMessage(any(), any());
			entityService.updateMessage("urn:ngsi-ld:Vehicle:A103", updatePayload);
			verify(entityService, times(1)).updateMessage(any(),any());

		
	}
	
	/**
	 * this method is use for append the field or attribute in entity
	 */
	@Test
	public void appendFieldTest(){
		try {
			Mockito.doReturn(blankNode).when(objectMapper).createObjectNode();
			Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(String.class));
			
			AppendResult appendResult=entityService.appendFields(entityPayload, appendJsonNode, " ");
			
			Assert.assertTrue(appendResult.getStatus());
		}catch(Exception ex) {
			Assert.fail();
		}
	}
	
	/**
	 * this method is use for the update attribute field
	 */
	@Test
	public void updateAttributeFieldTest() {
		try {
			Mockito.doReturn(blankNode).when(objectMapper).createObjectNode();
			Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(String.class));
			

			UpdateResult updateResult=entityService.updateFields(entityPayload,updateJsonNode , null);
  		Assert.assertTrue(updateResult.getStatus());
			Assert.assertEquals(updateJsonNode, updateResult.getJsonToAppend());
		}catch (Exception ex) {
			Assert.fail();
		}
	}
	
	/**
	 * this method is use for the update partial attribute field
	 */
	@Test
	public void updatePartialAttributeFieldTest() {
		try {
			Mockito.doReturn(blankNode).when(objectMapper).createObjectNode();
			Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(String.class));
			AppendResult appendResult=entityService.appendFields(entityPayload, updateJsonNode, " ");
			Assert.assertTrue(appendResult.getStatus());
		}catch(Exception ex) {
			Assert.fail();
		}
	}
	

	public void updatePartialDefaultAttributeFieldTest() {
		try {
			Mockito.doReturn(blankNode).when(objectMapper).createObjectNode();
			Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(String.class));
			UpdateResult updateResult=entityService.updateFields(entityPayload,updatePartialDefaultAttributesNode , "http://example.org/vehicle/speed");
			Assert.assertTrue(updateResult.getStatus());
			Assert.assertEquals(updatePartialDefaultAttributesNode, updateResult.getJsonToAppend());
		}catch (Exception ex) {
			Assert.fail();
		}
	}
	

	/**
	 * this method is use for the datasetId is exist in case of delete the attribute instance
	 */
	@Test
	public void deleteAttributeInstanceIfDatasetIdExistTest() {
		try {
			Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(String.class));
			entityService.deleteFields(entityPayload, "http://example.org/vehicle/speed","urn:ngsi-ld:Property:speedometerA4567-speed",null);
		} catch (Exception ex) {
			Assert.fail();
		}
	}
	
	/**
	 * this method is use for all delete the attribute
	 */
	@Test
	public void deleteAllAttributeInstanceTest() {
		try {
			Mockito.doReturn(payloadNode).when(objectMapper).readTree(any(String.class));
			entityService.deleteFields(entityPayload, "http://example.org/vehicle/speed",null,"true");
		} catch (Exception ex) {
			Assert.fail();
		}
	}
}
