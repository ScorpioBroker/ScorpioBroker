package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.time.LocalDateTime;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.vertx.mutiny.sqlclient.Tuple;

public class TemporalQueryTerm {
	String timeProperty;
	String timerel;
	String timeAt;
	String endTimeAt;

	public TemporalQueryTerm(String timeProperty, String timerel, String timeAt, String endTimeAt) {
		this.timeProperty = timeProperty;
		this.timerel = timerel;
		this.timeAt = timeAt;
		this.endTimeAt = endTimeAt;
	}

	public String getTimerel() {
		return timerel;
	}

	public void setTimerel(String timerel) {
		this.timerel = timerel;
	}

	public String getTimeProperty() {
		return timeProperty;
	}

	public void setTimeProperty(String timeProperty) {
		this.timeProperty = timeProperty;
	}

	public String getTimeAt() {
		return timeAt;
	}

	public void setTimeAt(String timeAt) {
		this.timeAt = timeAt;
	}

	public String getEndTimeAt() {
		return endTimeAt;
	}

	public void setEndTimeAt(String endTimeAt) {
		this.endTimeAt = endTimeAt;
	}

	public int toSql(StringBuilder sql, Tuple tuple, int dollarCount) {
		sql.append(getTimeProperty());
		switch (getTimerel()) {
		case NGSIConstants.TIME_REL_BEFORE:
			sql.append(" < $");
			sql.append(dollarCount);
			tuple.addLocalDateTime(LocalDateTime.parse(getTimeAt(), SerializationTools.informatter));
			dollarCount++;
			break;
		case NGSIConstants.TIME_REL_AFTER:
			sql.append(" > $");
			sql.append(dollarCount);
			tuple.addLocalDateTime(LocalDateTime.parse(getTimeAt(), SerializationTools.informatter));
			dollarCount++;
			break;
		case NGSIConstants.TIME_REL_BETWEEN:
			sql.append(" between $");
			sql.append(dollarCount);
			sql.append(" AND $");
			sql.append((dollarCount + 1));
			tuple.addLocalDateTime(LocalDateTime.parse(getTimeAt(), SerializationTools.informatter));
			tuple.addLocalDateTime(LocalDateTime.parse(getEndTimeAt(), SerializationTools.informatter));
			dollarCount += 2;
			break;
		}
		return dollarCount;
	}

}
