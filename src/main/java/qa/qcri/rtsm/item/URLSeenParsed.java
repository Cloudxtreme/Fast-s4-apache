package qa.qcri.rtsm.item;

import net.htmlparser.jericho.Source;

import org.apache.commons.cli.ParseException;
import org.json.JSONObject;

import qa.qcri.rtsm.util.WebUtil;

import com.google.common.collect.ImmutableMap;

public class URLSeenParsed extends URLSeen {

	private static final String DEFAULT_TITLE = "(untitled)";

	private static final String DEFAULT_OG_IMAGE = "about:blank";

	private static final String DEFAULT_CONTENT = "None";

	protected String title;

	protected String ogImage;

	protected String content;

	public URLSeenParsed() {

	}

	public URLSeenParsed(String site, String url, String htmlString) {
		super(site, url);

		if (htmlString == null || htmlString.length() == 0) {
			this.title = DEFAULT_TITLE;
			this.ogImage = DEFAULT_OG_IMAGE;
			this.content = DEFAULT_CONTENT;
		} else {
			Source htmlParsed = WebUtil.getParsedHTML(htmlString);
			this.content = WebUtil.getRawText(htmlString);
			try {
				this.title = WebUtil.getTitleOfParsedHTML(htmlParsed);
			} catch (ParseException e) {
				this.title = DEFAULT_TITLE;
			}
			try {
				this.ogImage = WebUtil.getOGImageOfParsedHTML(htmlParsed);
			} catch (ParseException e) {
				this.ogImage = DEFAULT_OG_IMAGE;
			}
		}
	}

	@Override
	public String toString() {
		return (new JSONObject(ImmutableMap.of("site", site, "url", url, "title", title, "ogImage", ogImage))).toString();

	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getOgImage() {
		return ogImage;
	}

	public void setOgImage(String ogImage) {
		this.ogImage = ogImage;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}
