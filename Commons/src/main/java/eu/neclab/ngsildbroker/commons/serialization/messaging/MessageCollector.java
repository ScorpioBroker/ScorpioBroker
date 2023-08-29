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
	private HashMap<Integer, List<byte[]>> messageId2Collector = Maps.newHashMap();
	private HashMap<Integer, Integer> messageId2MessageLength = Maps.newHashMap();
	private HashSet<Integer> completenessAttempted = Sets.newHashSet();
	private HashMap<Integer, Long> id2LastWrite = Maps.newHashMap();

	public void collect(byte[] input, CollectMessageListener listener) {

		ByteBuffer buffer = ByteBuffer.wrap(input);
		char firstChar = buffer.getChar();
		if (firstChar == '#') {
			int id = buffer.getInt();
			int nrChunks = buffer.getInt();
			List<byte[]> collector = messageId2Collector.get(id);
			if (collector == null) {
				collector = new ArrayList<>(nrChunks);
			}
			byte[] result = new byte[input.length - 9];
			buffer.get(9, result);
			collector.set(0, result);
			id2LastWrite.put(id, System.currentTimeMillis());
			messageId2MessageLength.put(id, nrChunks);

			if (completenessAttempted.contains(id)) {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				if (checkForCompleteness(bos, collector, id)) {
					listener.collected(bos.toByteArray());
				}
			}
		} else if (firstChar == '$') {
			int id = buffer.getInt();
			int pos = buffer.getInt();
			List<byte[]> collector = messageId2Collector.get(id);
			if (collector == null) {
				collector = new ArrayList<>();
			}
			byte[] result = new byte[input.length - 9];
			buffer.get(9, result);
			collector.set(pos, result);
			id2LastWrite.put(id, System.currentTimeMillis());
			if (completenessAttempted.contains(id)) {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				if (checkForCompleteness(bos, collector, id)) {
					listener.collected(bos.toByteArray());
				}
			}
		} else if (firstChar == '%') {
			int id = buffer.getInt();
			int pos = buffer.getInt();
			List<byte[]> collector = messageId2Collector.get(id);
			if (collector == null) {
				collector = new ArrayList<>();
			}
			byte[] result = new byte[input.length - 9];
			buffer.get(9, result);
			collector.set(pos, result);
			id2LastWrite.put(id, System.currentTimeMillis());
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			if (checkForCompleteness(bos, collector, id)) {
				sendResult(id, listener, bos.toByteArray());
			}

		} else {
			listener.collected(input);
		}
	}

	private void sendResult(int id, CollectMessageListener listener, byte[] byteArray) {
		messageId2Collector.remove(id);
		messageId2MessageLength.remove(id);
		completenessAttempted.remove(id);
		id2LastWrite.remove(id);
		listener.collected(byteArray);
	}

	private boolean checkForCompleteness(ByteArrayOutputStream bos, List<byte[]> collector, int id) {
		for (byte[] part : collector) {
			if (part == null) {
				// incomplete no action
				completenessAttempted.add(id);
				return false;
			}
			try {
				bos.write(part);
			} catch (IOException e) {
				logger.error("failed to construct message", e);
				return false;
			}
		}
		return true;
	}

}
