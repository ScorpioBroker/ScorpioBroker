package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.io.Serializable;

import com.github.jsonldjava.core.Context;

public class CSFQueryTerm extends QQueryTerm implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 707863320739851119L;

	CSFQueryTerm() {
		// for serialization
	}

	public CSFQueryTerm(Context context) {
		super(context);
	}

}
