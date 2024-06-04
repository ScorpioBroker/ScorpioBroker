package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.smallrye.mutiny.tuples.Tuple2;

public class LanguageQueryTerm implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3970320741960527943L;

	public LanguageQueryTerm() {
		// for serialization
	}

	ArrayList<Tuple2<Set<String>, Float>> entries = Lists.newArrayList();

	public ArrayList<Tuple2<Set<String>, Float>> getEntries() {
		return entries;
	}

	public void setEntries(ArrayList<Tuple2<Set<String>, Float>> entries) {
		this.entries = entries;
	}

	public void addTuple(Tuple2<Set<String>, Float> tuple) {
		this.entries.add(tuple);
	}

	public void sort() {
		this.entries.sort((o1, o2) -> {
			return o1.getItem2().compareTo(o2.getItem2());
		});
	}

	public void calculateQuery(List<Map<String, Object>> queryResult) {
		for (Map<String, Object> entity : queryResult) {
			for (Entry<String, Object> attrib : entity.entrySet()) {
				Object attribValueObj = attrib.getValue();
				if (attribValueObj instanceof List list) {
					for (Object entry : list) {
						if (entry instanceof Map map) {
							Object types = map.get(NGSIConstants.JSON_LD_TYPE);
							if (types != null && types instanceof List typeList
									&& typeList.contains(NGSIConstants.LANGUAGE_PROPERTY)) {
								Object hasLanguageMap = map.get(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP);
								if (hasLanguageMap != null && hasLanguageMap instanceof List languageMapEntries) {
									Iterator it = languageMapEntries.iterator();
									float bestFound = 0;
									while (it.hasNext()) {
										Object next = it.next();
										Map<String, String> languageMapEntry = (Map<String, String>) next;
										String lang = languageMapEntry.get(NGSIConstants.JSON_LD_LANGUAGE);
										boolean remove = true;
										for (Tuple2<Set<String>, Float> langQEntry : entries) {
											if (langQEntry.getItem2() > bestFound
													&& langQEntry.getItem1().contains(lang)) {
												remove = false;
												break;
											}
										}
										if (remove) {
											it.remove();
										}
									}
								}
							}
						}

					}
				}

			}
		}
	}

	public void toRequestString(StringBuilder result) {
		result.append("lang=");
		for (Tuple2<Set<String>, Float> entry : entries) {
			result.append(String.join(",", entry.getItem1()));
			result.append(";q=");
			result.append(entry.getItem2());
			result.append(',');
		}
		result.setCharAt(result.length() - 1, '&');
	}

	public boolean calculateEntity(Map<String, Object> entity) {
		for (Entry<String, Object> attrib : entity.entrySet()) {
			Object attribValueObj = attrib.getValue();
			if (attribValueObj instanceof List list) {
				for (Object entry : list) {
					if (entry instanceof Map map) {
						Object types = map.get(NGSIConstants.JSON_LD_TYPE);
						if (types != null && types instanceof List typeList
								&& typeList.contains(NGSIConstants.LANGUAGE_PROPERTY)) {
							Object hasLanguageMap = map.get(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP);
							if (hasLanguageMap != null && hasLanguageMap instanceof List languageMapEntries) {
								Iterator it = languageMapEntries.iterator();
								float bestFound = 0;
								while (it.hasNext()) {
									Object next = it.next();
									Map<String, String> languageMapEntry = (Map<String, String>) next;
									String lang = languageMapEntry.get(NGSIConstants.JSON_LD_LANGUAGE);
									boolean remove = true;
									for (Tuple2<Set<String>, Float> langQEntry : entries) {
										if (langQEntry.getItem2() > bestFound && langQEntry.getItem1().contains(lang)) {
											remove = false;
											break;
										}
									}
									if (remove) {
										it.remove();
									}
								}
							}
						}
					}

				}
			}

		}

		return !entity.isEmpty();
	}

}
