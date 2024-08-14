package eu.neclab.ngsildbroker.commons.serialization.messaging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.InflaterOutputStream;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;

public class BaseRequestDeserializer extends JsonDeserializer<BaseRequest> {

	private static final Decoder base64Decoder = Base64.getDecoder();
	private static final ObjectMapper mapper = new ObjectMapper();
	private static final TypeReference<Map<String, Object>> mapTypeRef = new TypeReference<Map<String, Object>>() {
	};

	@Override
	public BaseRequest deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
		JsonNode node = p.getCodec().readTree(p);

		BaseRequest result = new BaseRequest();
		boolean zipped = node.get(AppConstants.ZIPPED_SERIALIZATION_CHAR).asBoolean();
		result.setZipped(zipped);

		JsonNode tmp = node.get(AppConstants.TENANT_SERIALIZATION_CHAR);
		if (tmp != null) {
			result.setTenant(tmp.asText());
		}
		tmp = node.get(AppConstants.REQUESTTYPE_SERIALIZATION_CHAR);
		if (tmp != null) {
			result.setRequestType(tmp.asInt());
		}
		tmp = node.get(AppConstants.SENDTIMESTAMP_SERIALIZATION_CHAR);
		if (tmp != null) {
			result.setSendTimestamp(tmp.asLong());
		}
		tmp = node.get(AppConstants.ATTRIBNAME_SERIALIZATION_CHAR);
		if (tmp != null) {
			result.setAttribName(tmp.asText());
		}
		tmp = node.get(AppConstants.DATASETID_SERIALIZATION_CHAR);
		if (tmp != null) {
			result.setDatasetId(tmp.asText());
		}
		tmp = node.get(AppConstants.DELETEALL_SERIALIZATION_CHAR);
		if (tmp != null) {
			result.setDeleteAll(tmp.asBoolean());
		}
		tmp = node.get(AppConstants.DISTRIBUTED_SERIALIZATION_CHAR);
		if (tmp != null) {
			result.setDistributed(tmp.asBoolean());
		}
		tmp = node.get(AppConstants.NOOVERWRITE_SERIALIZATION_CHAR);
		if (tmp != null) {
			result.setNoOverwrite(tmp.asBoolean());
		}
		tmp = node.get(AppConstants.INSTANCEID_SERIALIZATION_CHAR);
		if (tmp != null) {
			result.setInstanceId(tmp.asText());
		}
		tmp = node.get(AppConstants.PAYLOAD_SERIALIZATION_CHAR);
		ArrayNode payloadArray = (ArrayNode) tmp;
		Map<String, List<Map<String, Object>>> payloadMap = Maps.newHashMap();
		Map<String, List<Map<String, Object>>> prevPayloadMap = Maps.newHashMap();
		Set<String> ids = Sets.newHashSet();
		for (int i = 0; i < payloadArray.size(); i += 3) {
			String entityId = payloadArray.get(i).asText();
			ids.add(entityId);
			Map<String, Object> payload;
			Map<String, Object> prevPayload;
			if (zipped) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				InflaterOutputStream inflater = new InflaterOutputStream(baos);

				inflater.write(base64Decoder.decode(payloadArray.get(i + 1).asText()));
				inflater.flush();
				inflater.close();
				baos.flush();
				byte[] payloadBytes = baos.toByteArray();
				baos.close();
				payload = mapper.readValue(payloadBytes, mapTypeRef);
				baos = new ByteArrayOutputStream();
				inflater = new InflaterOutputStream(baos);

				inflater.write(base64Decoder.decode(payloadArray.get(i + 2).asText()));
				inflater.flush();
				inflater.close();
				baos.flush();
				payloadBytes = baos.toByteArray();
				baos.close();
				prevPayload = mapper.readValue(payloadBytes, mapTypeRef);

			} else {
				payload = mapper.convertValue(payloadArray.get(i + 1), mapTypeRef);
				prevPayload = mapper.convertValue(payloadArray.get(i + 2), mapTypeRef);
			}
			if (payload != null) {
				MicroServiceUtils.putIntoIdMap(payloadMap, entityId, payload);
			}
			if (prevPayload != null) {
				MicroServiceUtils.putIntoIdMap(prevPayloadMap, entityId, prevPayload);
			}
		}
		if (!payloadMap.isEmpty()) {
			result.setPayload(payloadMap);
		}
		if (!prevPayloadMap.isEmpty()) {
			result.setPrevPayload(prevPayloadMap);
		}
		result.setIds(ids);
		return result;
	}

}
