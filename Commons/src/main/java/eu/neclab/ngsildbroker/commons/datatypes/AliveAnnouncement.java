package eu.neclab.ngsildbroker.commons.datatypes;

import eu.neclab.ngsildbroker.commons.interfaces.AnnouncementMessage;

public class AliveAnnouncement implements AnnouncementMessage {

	private String id;

	public AliveAnnouncement() {
		// for serialization
	}

	public AliveAnnouncement(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "AliveAnnouncement [id=" + id + "]";
	}

}
