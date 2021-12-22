package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.HashMap;

public class GenericBaseProperty extends BaseProperty {
	HashMap<String, ? extends BaseEntry> entries;
	private boolean multiValue;

	public GenericBaseProperty(HashMap<String, ? extends BaseEntry> entries, boolean multiValue) {
		this.entries = entries;
		this.multiValue = multiValue;
	}

	@Override
	public boolean isMultiValue() {
		return multiValue;
	}

	@Override
	public HashMap<String, ? extends BaseEntry> getEntries() {
		return entries;
	}

}
