package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;

public class AliveAnnouncement implements Serializable {
	
	public static final int NORMAL_SUB = 0;
	public static final int REG_SUB = 1;

	/**
	 *
	 */
	private static final long serialVersionUID = 6312100677371638447L;
	private String id;
	private int subType;

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

	public int getSubType() {
		return subType;
	}

	public void setSubType(int subType) {
		this.subType = subType;
	}

	@Override
	public String toString() {
		return "AliveAnnouncement [id=" + id + ", subType=" + subType + "]";
	}

}
