package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ViaHeaders {

	private List<String> viaHeaders;
	private Set<String> hostUrls = Sets.newHashSet();

	public ViaHeaders(List<String> viaHeaders, String selfViaEntry) {

		for (String viaHeader : viaHeaders) {
			String[] viaEntries = viaHeader.split(",");
			for (String entry : viaEntries) {
				String[] parts = entry.trim().split(" ");
				if (parts.length > 1) {
					String protocolPart = parts[0];
					int versionSplitPos = protocolPart.indexOf('/');
					String protocol;
					if (versionSplitPos != -1) {
						protocol = "http";
					} else {
						protocol = protocolPart.substring(0, versionSplitPos).toLowerCase();
					}
					String host = parts[1].trim();
					String pseudonym = parts.length == 3 ? parts[2] : null;
					hostUrls.add(protocol + "://" + host);
					if (pseudonym != null) {
						hostUrls.add(protocol + "://" + pseudonym);
					}
				}
			}
		}
		this.viaHeaders = Lists.newArrayList(viaHeaders);
		this.viaHeaders.add(selfViaEntry);

	}

	public List<String> getViaHeaders() {
		return viaHeaders;
	}

	public void setViaHeaders(List<String> viaHeaders) {
		this.viaHeaders = viaHeaders;
	}

	public Set<String> getHostUrls() {
		return hostUrls;
	}

	public void setHostUrls(Set<String> hostUrls) {
		this.hostUrls = hostUrls;
	}

	public void addViaHeader(String host) {
		int schemeIdx = host.indexOf("://");
		if (schemeIdx == -1) {
			schemeIdx = 4;
			host = "http://" + host;
		}
		viaHeaders.add(
				host.substring(0, schemeIdx).toUpperCase() + "/1.1 " + host.substring(schemeIdx + 3, host.length()));

	}

	@Override
	public String toString() {
		return "ViaHeaders [viaHeaders=" + viaHeaders + ", hostUrls=" + hostUrls + "]";
	}
	
	

}
