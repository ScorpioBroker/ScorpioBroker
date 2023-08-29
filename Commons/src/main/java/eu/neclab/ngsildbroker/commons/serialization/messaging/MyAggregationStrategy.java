package eu.neclab.ngsildbroker.commons.serialization.messaging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.RuntimeCamelException;

public class MyAggregationStrategy implements AggregationStrategy {

	@Override
	public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
		if (oldExchange == null) {
			// first time so create a new output stream to store the data
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			newExchange.getIn().setBody(bos);
			return newExchange;
		}

		// append the data from the new chunk to the existing output stream
		ByteArrayOutputStream bos = oldExchange.getIn().getBody(ByteArrayOutputStream.class);
		byte[] data = newExchange.getIn().getBody(byte[].class);
		try {
			bos.write(data);
		} catch (IOException e) {
			throw new RuntimeCamelException(e);
		}

		// check for completion
		boolean complete = newExchange.getIn().getHeader("CamelSplitComplete", Boolean.class);
		if (complete) {
			// set the output stream as the new body and close it
			newExchange.getIn().setBody(bos.toByteArray());
			try {
				bos.close();
			} catch (IOException e) {
				throw new RuntimeCamelException(e);
			}
		} else {
			// keep using the output stream as the body
			newExchange.getIn().setBody(bos);
		}

		return newExchange;
	}
}
