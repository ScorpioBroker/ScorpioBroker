package eu.neclab.ngsildbroker.commons.ldcontext;

public class CompactedJson {
	
	private String compacted;
	private String compactedWithContext;
	private String contextUrl;
	private Object compactedObject;
	
	
	

	public Object getCompactedObject() {
		return compactedObject;
	}

	public String getCompacted() {
		return compacted;
	}

	public void setCompacted(String compacted) {
		this.compacted = compacted;
	}

	public String getCompactedWithContext() {
		return compactedWithContext;
	}

	public void setCompactedWithContext(String compactedWithContext) {
		this.compactedWithContext = compactedWithContext;
	}

	public String getContextUrl() {
		return contextUrl;
	}

	public void setContextUrl(String contextUrl) {
		this.contextUrl = contextUrl;
	}

	public void setCompactedObject(Object compactedObject) {
		this.compactedObject = compactedObject;
		
	}

	
	
	

}
