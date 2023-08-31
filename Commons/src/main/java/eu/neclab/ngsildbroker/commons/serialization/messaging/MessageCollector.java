package eu.neclab.ngsildbroker.commons.serialization.messaging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MessageCollector {

	private final static Logger logger = LoggerFactory.getLogger(MessageCollector.class);
	private HashMap<String, List<String>> messageId2Collector = Maps.newHashMap();
	private HashMap<String, Integer> messageId2MessageLength = Maps.newHashMap();
	private HashSet<String> completenessAttempted = Sets.newHashSet();
	private HashMap<String, Long> id2LastWrite = Maps.newHashMap();

	public void collect(String input, CollectMessageListener listener) {

		char firstChar = input.charAt(0);
		if (firstChar == '#') {
			String id = input.substring(1, 12);
			int nrChunks = Integer.parseInt(input.substring(12, 23));
			List<String> collector = messageId2Collector.get(id);
			String result = input.substring(23);
			if (collector == null) {
				collector = new ArrayList<>(nrChunks);
				collector.add(result);
				messageId2Collector.put(id, collector);
			} else {
				collector.set(0, result);
			}

			id2LastWrite.put(id, System.currentTimeMillis());
			messageId2MessageLength.put(id, nrChunks);

			if (completenessAttempted.contains(id)) {
				StringBuilder bos = new StringBuilder();
				if (checkForCompleteness(bos, collector, id)) {
					listener.collected(bos.toString());
				}
			}
		} else if (firstChar == '$') {
			String id = input.substring(1, 12);
			int pos = Integer.parseInt(input.substring(12, 23));
			List<String> collector = messageId2Collector.get(id);
			if (collector == null) {
				collector = new ArrayList<>();
				messageId2Collector.put(id, collector);
			}
			String result = input.substring(23);
			if (collector.size() == pos) {
				collector.add(result);
			} else if (collector.size() > pos) {
				collector.set(pos, result);
			} else {
				for (int i = collector.size(); i <= pos; i++) {
					collector.add(null);
				}
				collector.set(pos, result);
			}
			id2LastWrite.put(id, System.currentTimeMillis());
			if (completenessAttempted.contains(id)) {
				StringBuilder bos = new StringBuilder();
				if (checkForCompleteness(bos, collector, id)) {
					listener.collected(bos.toString());
				}
			}
		} else if (firstChar == '%') {
			String id = input.substring(1, 12);
			int pos = Integer.parseInt(input.substring(12, 23));
			List<String> collector = messageId2Collector.get(id);
			if (collector == null) {
				collector = new ArrayList<>();
				messageId2Collector.put(id, collector);
			}
			String result = input.substring(23);
			if (collector.size() - 1 == pos) {
				collector.add(result);
			} else if (collector.size() - 1 > pos) {
				collector.set(pos, result);
			} else {
				for (int i = collector.size(); i <= pos; i++) {
					collector.add(null);
				}
				collector.set(pos, result);
			}
			id2LastWrite.put(id, System.currentTimeMillis());
			StringBuilder bos = new StringBuilder();
			if (checkForCompleteness(bos, collector, id)) {
				sendResult(id, listener, bos.toString());
			}

		} else {
			listener.collected(input);
		}
	}

	private void sendResult(String id, CollectMessageListener listener, String message) {
		messageId2Collector.remove(id);
		messageId2MessageLength.remove(id);
		completenessAttempted.remove(id);
		id2LastWrite.remove(id);
		listener.collected(message);
	}

	private boolean checkForCompleteness(StringBuilder bos, List<String> collector, String id) {
		for (String part : collector) {
			if (part == null) {
				// incomplete no action
				completenessAttempted.add(id);
				return false;
			}
			bos.append(part);
		}
		return true;
	}

}
