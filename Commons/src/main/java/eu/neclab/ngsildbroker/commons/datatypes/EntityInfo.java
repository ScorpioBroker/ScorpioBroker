package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.Serializable;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:34:28
 */
public class EntityInfo implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -1589924178919349697L;
	private String[] id;
	private String idPattern;
	private TypeQueryTerm typeTerm;

	public EntityInfo() {

	}

	public EntityInfo(String[] id, String idPattern, TypeQueryTerm typeTerm) {
		super();
		this.id = id;
		this.idPattern = idPattern;
		this.typeTerm = typeTerm;
	}

	public String[] getId() {
		return id;
	}

	public void setId(String[] id) {
		this.id = id;
	}

	public String getIdPattern() {
		return idPattern;
	}

	public void setIdPattern(String idPattern) {
		this.idPattern = idPattern;
	}

	public void finalize() throws Throwable {

	}

	@Override
	public String toString() {
		return "EntityInfo [id=" + id + ", idPattern=" + idPattern + ", typeTerm=" + typeTerm + "]";
	}

	public TypeQueryTerm getTypeTerm() {
		return typeTerm;
	}

	public void setTypeTerm(TypeQueryTerm typeTerm) {
		this.typeTerm = typeTerm;
	}

}