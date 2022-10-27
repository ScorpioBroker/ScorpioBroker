package eu.neclab.ngsildbroker.commons.datatypes;

public class BatchInfo {
	int batchId;
	int batchSize;

	public BatchInfo() {

	}

	public BatchInfo(int batchId, int batchSize) {
		this.batchId = batchId;
		this.batchSize = batchSize;
	}

	public int getBatchId() {
		return batchId;
	}

	public void setBatchId(int batchId) {
		this.batchId = batchId;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

}
