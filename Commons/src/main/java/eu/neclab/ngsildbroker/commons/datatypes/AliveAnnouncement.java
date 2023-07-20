package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
@JsonDeserialize
public class AliveAnnouncement implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 6312100677371638447L;
	private String id;

	public AliveAnnouncement() {
		// for serialization
	}
	@JsonCreator
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
