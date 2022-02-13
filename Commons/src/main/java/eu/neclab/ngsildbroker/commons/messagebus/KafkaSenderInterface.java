package eu.neclab.ngsildbroker.commons.messagebus;

public interface KafkaSenderInterface {

	void newMessage(String topic, String key, Object object);

}
