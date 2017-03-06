package qa.qcri.rtsm.item;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class SiteConfigurations {
	protected List<SiteConfiguration> sites;
	
	protected HashMap<String,SiteConfiguration> siteToConfig;
	
	public List<SiteConfiguration> getSites() {
		return sites;
	}

	public void setSites(List<SiteConfiguration> sites) {
		this.sites = sites;
		siteToConfig = new HashMap<String,SiteConfiguration>();
		for( SiteConfiguration siteConfig: sites ) {
			siteToConfig.put(siteConfig.getSiteId(), siteConfig);
		}
	}
	
	public SiteConfiguration getSiteById(String siteId) {
		return siteToConfig.get(siteId);
	}
	
	public boolean hasSiteId(String siteId) {
		return siteToConfig.containsKey(siteId);
	}

	public boolean hasURLPattern(String url, String siteId) {
		String URLPattern = getSiteById(siteId).getURLPattern();
		return url.contains(URLPattern);
	}
	public String listIds() {
		return StringUtils.join( siteToConfig.keySet(), ", " );
	}
}
