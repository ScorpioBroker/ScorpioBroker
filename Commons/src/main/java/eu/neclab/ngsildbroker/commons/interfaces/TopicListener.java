package eu.neclab.ngsildbroker.commons.interfaces;

public interface TopicListener {

	public void newMessage(String topic, String key, Object message);
}
