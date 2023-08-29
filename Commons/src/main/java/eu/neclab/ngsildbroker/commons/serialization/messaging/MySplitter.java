package eu.neclab.ngsildbroker.commons.serialization.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.camel.Exchange;

public class MySplitter {

	public static final int CHUNK_SIZE = 256 * 1024; // 256 KB

	public static Iterable<byte[]> splitBySize(Exchange exchange) {
		byte[] data = exchange.getIn().getBody(byte[].class);
		int size = data.length;
		int chunks = (int) Math.ceil((double) size / CHUNK_SIZE);
		List<byte[]> result = new ArrayList<>(chunks);
		for (int i = 0; i < chunks; i++) {
			int from = i * CHUNK_SIZE;
			int to = Math.min(from + CHUNK_SIZE, size);
			byte[] chunk = Arrays.copyOfRange(data, from, to);
			result.add(chunk);
		}
		exchange.setProperty("CorrelationID", data.hashCode());
		exchange.setProperty("CamelSplitSize", chunks);
		exchange.setProperty("CamelSplitIndex", 0);
		exchange.setProperty("CamelSplitComplete", false);
		return result;
	}
}