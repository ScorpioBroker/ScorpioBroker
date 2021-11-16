package com.github.jsonldjava.core;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.utils.JsonUtils;

public class Main {
	public static void main(String[] args) throws Exception {
		long[] exp1 = new long[20], exp2 = new long[20], comp1 = new long[20], comp2 = new long[20],
				read1 = new long[20], read2 = new long[20], read3 = new long[20], read4 = new long[20];
		for (int i = 0; i < 20; i++) {

			long temp1, temp2, temp3, temp4;
			String entity = "{\n" + "  \"id\" : \"EmissionObserved:1626696072466" + i + "\",\n"
					+ "  \"type\" : \"EmissionObserved\",\n"
					+ "  \"https://smart-data-models.github.io/data-models/terms.jsonld#/definitions/co2\" : {\n"
					+ "    \"type\" : \"Property\",\n" + "    \"value\" : 1.0566390167028306,\n"
					+ "    \"observedAt\" : \"2021-07-19T12:01:12.466Z\"\n" + "  },\n" + "  \"abstractionLevel\" : {\n"
					+ "    \"type\" : \"Property\",\n" + "    \"value\" : 17\n" + "  },\n" + "  \"location\" : {\n"
					+ "    \"type\" : \"GeoProperty\",\n" + "    \"value\" : {\n" + "      \"type\" : \"Polygon\",\n"
					+ "      \"coordinates\" : [[[ 8.6447165, 49.4184066 ],[ 8.6447165, 49.4184066 ],[ 8.6447165, 49.4184066 ],[ 8.6447165, 49.4184066 ]]]\n"
					+ "    },\n" + "    \"observedAt\" : \"2021-07-19T12:01:12.466Z\"\n" + "  }\n" + "}";

			temp1 = System.currentTimeMillis();
			Object obj = JsonUtils.fromString(entity);
			((Map<String, Object>) obj).put("@context", "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld");
			List<Object> bla1 = JsonLdProcessor.expand(obj);
			temp2 = System.currentTimeMillis();
			exp2[i] = temp2 - temp1;
			System.out.println(JsonUtils.toPrettyString(bla1));
			Map<String, Object> compacted = JsonLdProcessor.compact(bla1, "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld", new JsonLdOptions(JsonLdOptions.JSON_LD_1_1));
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
