package eu.neclab.ngsildbroker.commons.datatypes.terms;


import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import io.vertx.mutiny.sqlclient.Tuple;



public abstract class ProjectionTerm {

	protected boolean hasLinked = false;
	protected ProjectionTerm next;
	protected ProjectionTerm linkedChild;
	protected ProjectionTerm parent;
	protected String attrib;

	public boolean isHasLinked() {
		return hasLinked;
	}

	public void setHasLinked(boolean hasLinked) {
		this.hasLinked = hasLinked;
	}

	public ProjectionTerm getNext() {
		return next;
	}

	public void setNext(ProjectionTerm next) {
		this.next = next;
	}

	public ProjectionTerm getLinkedChild() {
		return linkedChild;
	}

	public void setLinkedChild(ProjectionTerm linkedChild) {
		this.linkedChild = linkedChild;
	}

	public ProjectionTerm createNewNext() {
		next = getInstance();
		next.parent = parent;
		return next;
	}
	

	public ProjectionTerm getParent() {
		return parent;
	}

	public void setParent(ProjectionTerm parent) {
		this.parent = parent;
	}

	public ProjectionTerm createNewChild() {
		linkedChild = getInstance();
		return linkedChild;
	}

	public String getAttrib() {
		return attrib;
	}

	public void setAttrib(String attrib) {
		this.attrib = attrib;
	}

	protected abstract ProjectionTerm getInstance();
	
	public abstract int toSqlConstructEntity(StringBuilder query, Tuple tuple, int dollar, String tableToUse, DataSetIdTerm dataSetIdTerm);

	public abstract int toSql(StringBuilder query, Tuple tuple, int dollar);

	public abstract Map<String, Object> calculateEntity(Map<String, Object> entity);

}
