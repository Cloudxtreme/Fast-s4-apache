package qa.qcri.rtsm.item;


public abstract class URLSeen {

	protected String site;
	
	protected String url;
	
	public URLSeen() {

	}

	public URLSeen(String site, String url) {
		this.site = site;
		this.url = url;
	}

	public Object clone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException cnse) {
			throw new RuntimeException(cnse);
		}
	}

	public String getSite() {
		return site;
	}

	public void setSite(String site) {
		this.site = site;
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
}
