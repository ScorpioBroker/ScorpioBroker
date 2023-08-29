package eu.neclab.ngsildbroker.commons.serialization.messaging;

public interface CollectMessageListener {

	void collected(byte[] message);
}
