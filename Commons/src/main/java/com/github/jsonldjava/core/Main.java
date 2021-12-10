package com.github.jsonldjava.core;

import java.util.List;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class Main {
	public static void main(String[] args) throws Exception {
		long[] exp1 = new long[20], exp2 = new long[20], comp1 = new long[20], comp2 = new long[20],
				read1 = new long[20], read2 = new long[20], read3 = new long[20], read4 = new long[20];
		for (int i = 0; i < 20; i++) {

			long temp1, temp2, temp3, temp4;
			String subscription = "{\n" + "	\"id\": \"urn:ngsi-ld:Subscription:mySubscription\",\n"
					+ "	\"subscriptionName\": \"testname\",\n" + "	\"type\": \"Subscription\",\n"
					+ "	\"entities\": [{\n" + "			\"type\": \"Vehicle\"\n" + "		},\n" + "		{\n"
					+ "			\"id\": \"bla:asd\",\n" + "			\"type\": \"Vehicle\"\n" + "		},\n"
					+ "		{\n" + "			\"idPattern\": \"bla.*\",\n" + "			\"type\": \"Vehicle\"\n"
					+ "		}\n" + "	],\n" + "	\"description\": \"bla description\",\n"
					+ "	\"timeInterval\": 50,\n" + "	\"watchedAttributes\": [\"speed\", \"somemore\"],\n"
					+ "	\"q\": \"speed>50\",\n" + "	\"geoQ\": {\n" + "		\"georel\": \"near;maxDistance==2000\",\n"
					+ "		\"geometry\": \"Point\",\n" + "		\"coordinates\": [-1, 100]\n" + "	},\n"
					+ "	\"notification\": {\n" + "		\"attributes\": [\"speed\"],\n"
					+ "		\"format\": \"keyValues\",\n" + "		\"endpoint\": {\n"
					+ "			\"uri\": \"http://my.endpoint.org/notify\",\n"
					+ "			\"accept\": \"application/json\",\n" + "			\"receiverInfo\": [{\n"
					+ "				\"rbla1\": \"rbla2\"\n" + "			}, {\n" + "				\"rbla3\": \"rbla4\"\n"
					+ "			}],\n" + "			\"notifierInfo\": [{\n" + "				\"nbla1\": \"nbla2\"\n"
					+ "			}, {\n" + "				\"nbla3\": \"nbla4\"\n" + "			}]\n" + "		}\n"
					+ "	},\n" + "	\"isActive\": true,\n" + "	\"expiresAt\": \"2017-07-29T12:00:04Z\",\n"
					+ "	\"throttling\": 60,\n" + "	\"temporalQ\": {\n" + "		\"timerel\": \"between\",\n"
					+ "		\"timeproperty\": \"observedAt\",\n" + "		\"timeAt\": \"2017-07-29T12:00:04Z\",\n"
					+ "		\"endTimeAt\": \"2018-07-29T12:00:04Z\"\n" + "	},\n" + "	\"@context\": [\n"
					+ "		\"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.4.jsonld\"\n" + "	]\n" + "}";
			String entity2 = "{\n" + "	\"id\": \"urn:ngsi-ld:Vehicle:A4567\",\n" + "	\"type\": \"Vehicle\",\n"
					+ "	\"speed\": [{\n" + "			\"type\": \"Property\",\n" + "			\"value\": 55,\n"
					+ "			\"source\": {\n" + "				\"type\": \"Property\",\n"
					+ "				\"value\": \"Speedometer\"\n" + "			}\n" +
					// " \"datasetId\": \"urn:ngsi-ld:test\"\n" +
					"		},\n" + "		{\n" + "			\"type\": \"Property\",\n"
					+ "			\"value\": 54.5,\n" + "			\"source\": {\n"
					+ "				\"type\": \"Property\",\n" + "				\"value\": \"GPS\"\n" + "			}\n"
					+
					// " \"datasetId\": \"urn:ngsi-ld:test\"\n" +
					"		}\n" + "	],\n" + "	\"@context\": [\n"
					+ "		\"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.4.jsonld\"\n" + "	]\n" + "}";
			String entity1 = "{\n" + "	\"id\": \"urn:ngsi-ld:Vehicle:A4567\",\n" + "	\"type\": \"Vehicle\",\n" +

			// " \"testing123\": \"test\"," +
			// " \"testing456\": {\"test\":123}," +
					"	\"brandName\": {\"unitCode\": \"EUR\",\n" + "		\"type\": \"Property\",\n"
					+ "		\"value\": [{\"bla1\":\"Mercedes\"},{\"bla2\":\"Audi\"} ]\n" + ", \"address\": {\n"
					+ " \"type\":\"Property\",\n" + " \"value\": {\n" + " \"city\":\"Berlin\",\n"
					+ " \"street\":\"Ulrich Strasse\"\n" + " }\n" + " }	},\n" +
					/*
					 * "	\"street\": {\n" + "		\"type\": \"LanguageProperty\",\n" +
					 * "		\"languageMap\": {\n" + "			\"fr\": \"Grand Place\",\n" +
					 * "			\"nl\": \"Grote Markt\"\n" + "		}\n" + "	},\n" +
					 */
					"	\"isParked\": {\n" + "		\"type\": \"Relationship\",\n"
					+ "		\"object\": \"urn:ngsi-ld:OffStreetParking:Downtown1\",\n"
					+ "		\"observedAt\": \"2017-07-29T12:00:04Z\",\n" + "		\"providedBy\": {\n"
					+ "			\"type\": \"Relationship\",\n" + "			\"object\": \"urn:ngsi-ld:Person:Bob\"\n"
					+ "		}\n" + "	},\n" + "	\"location\": {\n" + "		\"type\": \"GeoProperty\",\n"
					+ "		\"value\": {\n" + "			\"type\": \"Polygon\",\n" + "			\"coordinates\": [\n"
					+ "				[\n" + "					[8.6447165, 49.4184066],\n"
					+ "					[8.6447165, 49.4184066],\n" + "					[8.6447165, 49.4184066],\n"
					+ "					[8.6447165, 49.4184066]\n" + "				]\n" + "			]\n"
					+ "		},\n" + "		\"observedAt\": \"2021-07-19T12:01:12.466Z\"\n" + "	}\n" + "}";
			/*
			 * "{\n" + "  \"id\" : \"EmissionObserved:1626696072466" + i + "\",\n" +
			 * "  \"type\" : \"EmissionObserved\",\n" +
			 * "  \"https://smart-data-models.github.io/data-models/terms.jsonld#/definitions/co2\" : {\n"
			 * + "    \"type\" : \"Property\",\n" + "    \"value\" : 1.0566390167028306,\n"
			 * + "    \"observedAt\" : \"2021-07-19T12:01:12.466Z\"\n" + "  },\n" +
			 * "  \"abstractionLevel\" : {\n" + "    \"type\" : \"Property\",\n" +
			 * "    \"value\" : 17\n" + "  },\n" + "  \"location\" : {\n" +
			 * "    \"type\" : \"GeoProperty\",\n" + "    \"value\" : {\n" +
			 * "      \"type\" : \"Polygon\",\n" +
			 * "      \"coordinates\" : [[[ 8.6447165, 49.4184066 ],[ 8.6447165, 49.4184066 ],[ 8.6447165, 49.4184066 ],[ 8.6447165, 49.4184066 ]]]\n"
			 * + "    },\n" + "    \"observedAt\" : \"2021-07-19T12:01:12.466Z\"\n" +
			 * "  }\n" + "}";
			 */
			// System.out.println(entity);
			temp1 = System.currentTimeMillis();
			Object obj = JsonUtils.fromString(entity1);

			List<Object> bla1 = JsonLdProcessor.expand(null, obj, new JsonLdOptions(JsonLdOptions.JSON_LD_1_1),
					AppConstants.ENTITY_CREATE_PAYLOAD, false);
			temp2 = System.currentTimeMillis();
			exp2[i] = temp2 - temp1;
			System.out.println(JsonUtils.toPrettyString(bla1));
			Map<String, Object> compacted = JsonLdProcessor.compact(bla1,
					"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld",
					new JsonLdOptions(JsonLdOptions.JSON_LD_1_1));
			System.out.println(JsonUtils.toPrettyString(compacted));
			/*
			 * String test2 = contextResolver.expand(entity, null, false, 1);
			 * 
			 * 
			 * temp1 = System.currentTimeMillis(); Entity entityTest =
			 * DataSerializer.getEntity(test2); temp2 = System.currentTimeMillis(); read4[i]
			 * = temp2 - temp1;
			 */

			/*
			 * temp3 = System.currentTimeMillis(); JsonLdProcessor.compact(bla,
			 * JsonUtils.fromInputStream( new
			 * URL("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld").openStream
			 * ()), new JsonLdOptions()); temp4 = System.currentTimeMillis(); comp2 = temp4
			 * - temp3;
			 * 
			 * System.out.println(comp1); System.out.println(comp2);
			 */

		}

		long avgexp1 = 0, avgexp2 = 0, avgread1 = 0, avgread2 = 0, avgread3 = 0, avgread4 = 0;
		for (int i = 0; i < comp2.length; i++) {
			System.out.println(exp2[i]);
			avgexp2 += exp2[i];
			System.out.println("-------------------");
		}
		System.out.println(avgexp2 / 20);
		System.out.println((avgexp2 - exp2[0]) / 19);

		// System.out.println(avgread4/20);
		// System.out.println(blub.toString());

	}
}