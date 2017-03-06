package qa.qcri.rtsm.item;

import java.net.MalformedURLException;
import java.net.URL;

import org.json.JSONObject;

import com.google.common.collect.ImmutableMap;

public class URLSeenSource extends URLSeen {

	private static final String DEFAULT_SOURCE = "(no source)";

	private static final String DEFAULT_SEARCH_TERMS = "(no search terms)";
	
	private static final String DEFAULT_REFERRAL = "(no referral)";

	protected String source;

	protected String searchTerms;
	
	protected String referral;
	
	public static enum SourceType {
		DIRECT("d"), 
		REFERRAL("r"),
		ORGANIC("o"),
		INTERNAL("i");
		
		String symbol;
		SourceType(String symbol) {
			this.symbol = symbol;
		}
		
		public String getSymbol() {
			return symbol;
		}
	}

	public URLSeenSource() {
		super();
		this.source = DEFAULT_SOURCE;
		this.searchTerms = DEFAULT_SEARCH_TERMS;
		this.referral = DEFAULT_REFERRAL;
	}

	public URLSeenSource(String site, String url, Visit visit) {
		super(site, url);
		this.source = visit.getSource();
		this.searchTerms = visit.getSearchTerms();
		this.referral = visit.getReferral();
	}
	
	public SourceType sourceType() {
		if( hasSearchTerms() ) {
			return SourceType.ORGANIC;
		} else if( hasReferralInSameHost() ) {
			return SourceType.INTERNAL;
		} else if( hasEmptyReferral() || hasNoTrafficSource() ) {
			return SourceType.DIRECT;
		} else {
			return SourceType.REFERRAL;
		}
	}
	
	private boolean hasReferralInSameHost() {
		if( referral != null && referral.length() > 0 ) {
			URL referringURL, myURL;
			try {
				referringURL = new URL(referral);
				myURL = new URL(url);
			} catch (MalformedURLException e) {
				e.printStackTrace();
				return false;
			}
			String referringHost = ( referringURL != null ) ? referringURL.getHost() : null;
			String myHost = ( myURL != null ) ? myURL.getHost() : null;
			if( referringHost != null && myHost != null && referringHost.equals(myHost) ) {
				return true;
			}
		}
		return false;
	}

	private boolean hasNoTrafficSource() {
		String src = this.getSource();
		return( src == null || src.length() == 0 || src.equals("(none)") || src.equals("%28none%29") );
	}
	
	private boolean hasEmptyReferral() {
		String ref = this.getReferral();
		return( ref == null || ref.length() == 0 );
	}
	
	private boolean hasSearchTerms() {
		String st = this.getSearchTerms();
		return( ! (st == null || st.length() == 0 || st.equals("(none)") || st.equals("%28none%29") || st.equals("(not provided)") || st.equals("%28not+provided%29") ) );
	}
	
	@Override
	public String toString() {
		return (new JSONObject(ImmutableMap.of("site", site, "url", url, "source", source, "searchTerms", searchTerms))).toString();

	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getSearchTerms() {
		return searchTerms;
	}

	public void setSearchTerms(String searchTerms) {
		this.searchTerms = searchTerms;
	}

	public String getReferral() {
		return referral;
	}

	public void setReferral(String referral) {
		this.referral = referral;
	}


}
