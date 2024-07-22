package eu.neclab.ngsildbroker.commons.datatypes.terms;


import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.mutiny.sqlclient.Tuple;



@JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "id")
public abstract class ProjectionTerm {
	@JsonIgnore
	private static SplittableRandom random = new SplittableRandom();
	protected int id = random.nextInt(); 
	protected boolean hasLinked = false;
	protected boolean hasAnyLinked = false;
	
	protected ProjectionTerm next;
	protected ProjectionTerm linkedChild;
	protected ProjectionTerm parent;
	protected String attrib;
	
	

	public boolean isHasAnyLinked() {
		return hasAnyLinked;
	}


	public void setHasAnyLinked(boolean hasAnyLinked) {
		this.hasAnyLinked = hasAnyLinked;
	}


	public boolean isHasLinked() {
		return hasLinked;
	}

	
	public int getId() {
		return id;
	}


	public void setId(int id) {
		this.id = id;
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
		linkedChild.setParent(this);
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
	
	public abstract boolean calculateEntity(Map<String, Object> entity, boolean inlineJoin, Map<String, Map<String, Object>> flatEntities);

}
