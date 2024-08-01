package eu.neclab.ngsildbroker.commons.datatypes.terms;


import java.util.Map;
import java.util.Set;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.google.common.collect.Sets;

import io.vertx.mutiny.sqlclient.Tuple;



@JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "id")
public abstract class ProjectionTerm {
	@JsonIgnore
	private static int idCount = 0;

	private static synchronized int nextId() {
		idCount++;
		return idCount;
	}
	protected int id = nextId(); 
	protected boolean hasLinked = false;
	protected boolean hasAnyLinked = false;
	
	protected ProjectionTerm next;
	protected ProjectionTerm linkedChild;
	protected ProjectionTerm parent;
	protected String attrib;
	
	private Set<String> allTopLevelAttribs;
	
	public Set<String> getAllTopLevelAttribs(boolean pick) {
		if (allTopLevelAttribs == null) {
			allTopLevelAttribs = Sets.newHashSet();
			ProjectionTerm current = this;
			while (current != null) {
				if (!current.hasLinked || pick) {
					allTopLevelAttribs.add(current.attrib);
				}
				current = current.next;
			}
		}
		return allTopLevelAttribs;

	}

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
	
	public abstract boolean calculateEntity(Map<String, Object> entity, boolean inlineJoin, Map<String, Map<String, Object>> flatEntities, Set<String> pickForFlat, boolean calculateLinked);

}
