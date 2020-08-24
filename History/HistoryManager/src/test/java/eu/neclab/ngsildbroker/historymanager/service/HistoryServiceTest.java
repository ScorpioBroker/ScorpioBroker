package eu.neclab.ngsildbroker.historymanager.service;

import java.net.URI;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;									
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.historymanager.config.ProducerChannel;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class HistoryServiceTest {
	
	@Autowired
    private MockMvc mockMvc;
	
	@Mock
	ProducerChannel producerChannels;
	
	@Mock
	KafkaOps kafkaOperations;
	
	@Mock
	HistoryDAO historyDAO;
	
	@Mock
	ParamsResolver paramsResolver;
	
	@InjectMocks
	@Spy
    private HistoryService historyService;
	
	URI uri;
	
	private String temporalPayload;
	
	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		uri=new URI(AppConstants.HISTORY_URL + "urn:ngsi-ld:testunit:151");
		
		temporalPayload="{\r\n  \"https://uri.etsi.org/ngsi-ld/default-context/airQualityLevel\" : [ "
				+ "{\r\n    \"https://uri.etsi.org/ngsi-ld/observedAt\" : [ "
				+ "{\r\n      \"@value\" : \"2018-08-07T12:00:00Z\","
				+ "\r\n      \"@type\" : \"https://uri.etsi.org/ngsi-ld/DateTime\""
				+ "\r\n    } ],"
				+ "\r\n    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],"
				+ "\r\n    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {"
				+ "\r\n      \"@value\" : \"good\""
				+ "\r\n    } ]"
				+ "\r\n  }, {"
				+ "\r\n    \"https://uri.etsi.org/ngsi-ld/observedAt\" : [ {"
				+ "\r\n      \"@value\" : \"2018-08-14T12:00:00Z\","
				+ "\r\n      \"@type\" : \"https://uri.etsi.org/ngsi-ld/DateTime\""
				+ "\r\n    } ],\r\n    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],"
				+ "\r\n    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {"
				+ "\r\n      \"@value\" : \"moderate\"\r\n    } ]"
				+ "\r\n  }, {"
				+ "\r\n    \"https://uri.etsi.org/ngsi-ld/observedAt\" : [ {"
				+ "\r\n      \"@value\" : \"2018-09-14T12:00:00Z\",\r\n      \"@type\" : \"https://uri.etsi.org/ngsi-ld/DateTime\"\r\n    } ],"
				+ "\r\n    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],"
				+ "\r\n    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {"
				+ "\r\n      \"@value\" : \"unhealthy\"\r\n    } ]"
				+ "\r\n  } ],"
				+ "\r\n  \"@id\" : \"urn:ngsi-ld:testunit:159\","
				+ "\r\n  \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/default-context/AirQualityObserved\" ]"
				+ "\r\n}";
	}
	
	/**
	 * this method is use test "createTemporalEntityFromBinding" method of HistoryService
	 */
	
	@Test
	public void createTemporalEntityFromBindingTest() {
		try {
			URI uri1=historyService.createTemporalEntityFromBinding(temporalPayload);
			verify(kafkaOperations, times(3)).pushToKafka(any(),any(),any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}		
	}
	
	/**
	 * this method is use test "createTemporalEntityFromEntity" method of HistoryService
	 */
	
	@Test
	public void createTemporalEntityFromEntityTest() {
		try {
			URI uri1=historyService.createTemporalEntityFromEntity(temporalPayload);
			verify(kafkaOperations, times(3)).pushToKafka(any(),any(),any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is use test "delete" method of HistoryService
	 */
	
	@Test
	public void deleteTemporalByIdTest() {
		List<Object> linkHeaders = null;
		try {
			Mockito.doReturn("https://uri.etsi.org/ngsi-ld/default-context/airQualityLevel").when(paramsResolver).expandAttribute(any(),any());
			historyService.delete("urn:ngsi-ld:testunit:151", "airQualityLevel", null, linkHeaders);
			verify(kafkaOperations, times(1)).pushToKafka(any(),any(),any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}		
	}
	
	/**
	 * this method is use test "addAttrib2TemporalEntity" method of HistoryService
	 */
	
	@Test
	public void addAttrib2TemporalEntityTest() {
		try {
			Mockito.doReturn(true).when(historyDAO).entityExists(any());
			historyService.addAttrib2TemporalEntity("urn:ngsi-ld:testunit:151", temporalPayload);
			verify(kafkaOperations, times(3)).pushToKafka(any(),any(),any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}	
	}
	
}
	