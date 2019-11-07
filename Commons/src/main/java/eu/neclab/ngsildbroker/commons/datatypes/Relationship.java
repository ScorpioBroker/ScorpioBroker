package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class Relationship extends BaseProperty {

	private URI object;
	

	public Relationship(){
		type = "Relationship";
	}

	public void finalize() throws Throwable {

	}

	public URI getObject() {
		return object;
	}

	public void setObject(URI object) {
		this.object = object;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((object == null) ? 0 : object.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Relationship other = (Relationship) obj;
		if (object == null) {
			if (other.object != null)
				return false;
		} else if (!object.equals(other.object))
			return false;
		return true;
	}

	
}