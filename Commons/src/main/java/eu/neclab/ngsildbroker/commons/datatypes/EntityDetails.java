package eu.neclab.ngsildbroker.commons.datatypes;

public class EntityDetails {

	private String key;
	private int partition;
	private long offset;
	
	public EntityDetails() {}

	
	public EntityDetails(String key, int partition, long offset) {
		super();
		this.key = key;
		this.partition = partition;
		this.offset = offset;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + (int) (offset ^ (offset >>> 32));
		result = prime * result + partition;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EntityDetails other = (EntityDetails) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (offset != other.offset)
			return false;
		if (partition != other.partition)
			return false;
		return true;
	}
	

	@Override
	public String toString() {
		return "EntityTopicDetails [key=" + key + ", partition=" + partition + ", offset=" + offset + "]";
	}
	

}
