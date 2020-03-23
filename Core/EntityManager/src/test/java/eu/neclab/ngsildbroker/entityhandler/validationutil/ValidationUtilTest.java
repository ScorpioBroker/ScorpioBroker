package eu.neclab.ngsildbroker.entityhandler.validationutil;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.Relationship;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;


@SpringBootTest(properties= {"spring.main.allow-bean-definition-overriding=true"})
@RunWith(SpringRunner.class)
public class ValidationUtilTest {
	
	TypeValidationRule typeValidation;
	PropertyValidatioRule propertyValidation;
	RelationshipValidationRule relationshipValidation;
	IdValidationRule idValidation;
	Entity entity;
	
	@Rule
	public ExpectedException thrown = ExpectedException.none();
	
	@Before
	public void setup() {
		typeValidation=new TypeValidationRule();
		propertyValidation=new PropertyValidatioRule();
		relationshipValidation=new RelationshipValidationRule();
		idValidation=new IdValidationRule();
		
		String payload="  {\r\n" + 
				"    \"http://example.org/vehicle/brandName\": [\r\n" + 
				"      {\r\n" +
				"        \"@type\": [\r\n" + 
				"          \"https://uri.etsi.org/ngsi-ld/Property\"\r\n" + 
				"        ],\r\n" + 
				"        \"https://uri.etsi.org/ngsi-ld/hasValue\": [\r\n" + 
				"          {\r\n" + 
				
				"        \"@value\": \"Volvo\"\r\n" + 
				"        }]\r\n" + 

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
		
		entity=DataSerializer.getEntity(payload);
	}
	
	@After
	public void tearDown() {
		typeValidation=null;
	}
	
	@Test
	public void typeValidationTest() {
		try {
			Assert.assertTrue(typeValidation.validateEntity(entity, null));
		}catch(Exception ex) {
			Assert.fail();
		}
	}
	
	@Test
	public void typeValidationFailureTest() throws ResponseException{
		thrown.expect(ResponseException.class);
		thrown.expectMessage("@type is not recognized");
		
		entity.setType("abc");
		typeValidation.validateEntity(entity, null);
	}
	
	@Test
	public void idValidationTest(){
		try {
			Assert.assertTrue(idValidation.validateEntity(entity, null));
		}catch(Exception ex) {
			Assert.fail();
		}
	}
	
	@Test
	public void idValidationFailureTest() throws ResponseException, URISyntaxException{
		thrown.expect(ResponseException.class);
		thrown.expectMessage("id is not a URI");
		
		entity.setId(new URI("abc"));
		idValidation.validateEntity(entity, null);
	}
	
	@Test
	public void propertyValidationTest() {
		try {
			Assert.assertTrue(propertyValidation.validateEntity(entity, null));
		}catch(Exception ex) {
			Assert.fail();
		}
	}
	
	@Test
	public void propertyValidationFailureTest() throws ResponseException {
		thrown.expect(ResponseException.class);
		thrown.expectMessage("Entity with a property value equal to null");
		
		entity.getProperties().get(0).setEntries(null);
		propertyValidation.validateEntity(entity, null);
		
	}
	
	@Test
	public void relationshipValidationTest(){
		try {
			Assert.assertTrue(relationshipValidation.validateEntity(entity, null));
		}catch(Exception ex) {
			Assert.fail();
		}
	}
	
	@Test
	public void relationshipValidationFailureTest() throws ResponseException {
		thrown.expect(ResponseException.class);
		thrown.expectMessage("Entity with a Relationship object equal to null");
		
		Relationship rel=new Relationship();
		rel.setObjects(null);
		ArrayList<Relationship> relsList=new ArrayList<>();
		relsList.add(rel);
		entity.setRelationships(relsList);
		
		entity.getRelationships().get(0).setObjects(null);
		relationshipValidation.validateEntity(entity, null);
		
	}
}
