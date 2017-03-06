package qa.qcri.rtsm.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.Source;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import com.google.common.net.InternetDomainName;
import java.net.URLDecoder;
public class WebUtil {

	/**
	 * Facebook's OpenGraph element 'image'
	 */
	private static final String META_PROPERTY = "property";
	private static final String OG_IMAGE_META_PROPERTY_VALUE = "og:image";
	private static final String OG_IMAGE_META_CONTENT_ATTRIBUTE = "content";

	public static Source getParsedHTML(String htmlContents) {
		InputStream instream = new ByteArrayInputStream(htmlContents.getBytes(Util.UTF8));

		Source source;
		try {
			source = new Source(instream);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		source.fullSequentialParse();
		return source;
	}

	public static String getRawText(String htmlContents){
		Source source = new Source(htmlContents);
		return source.getTextExtractor().toString();
	}

	private static List<Element> getElementsByNameOrRaiseException(Source source, String elementName) throws ParseException {
		List<Element> elements = source.getAllElements(elementName);
		if( elements.size() > 0 ) {
			return elements;
		} else {
			throw new ParseException("No element " + elementName + " found");
		}
	}
	
	private static String getFirstElementValue(Source source, String elementName) throws ParseException {
		List<Element> elements = getElementsByNameOrRaiseException(source, elementName);
		return elements.get(0).getTextExtractor().toString();
	}

	public static String getOGImageOfParsedHTML(Source source) throws ParseException {
		List<Element> metas = source.getAllElements(HTMLElementName.META);
		for( Element meta: metas ) {
			if( OG_IMAGE_META_PROPERTY_VALUE.equalsIgnoreCase(meta.getAttributeValue(META_PROPERTY)) ) {
				return meta.getAttributeValue(OG_IMAGE_META_CONTENT_ATTRIBUTE);
			}
		}
		return "";
	}
	
	public static String getTitleOfParsedHTML(Source source) throws ParseException {
		return getFirstElementValue(source, HTMLElementName.TITLE);
	}

	public static String getHTMLContentsAsString(String urlString) {
		HttpClient httpclient = new DefaultHttpClient();
		
		// Check URL
		URI url;
		try {
			url = new URI(urlString);
		} catch (URISyntaxException eUri) {
			Util.logWarning(new WebUtil(), "Syntax exception parsing URL '" + urlString + "'");
			eUri.printStackTrace();
			return null;
		}
		
		if( url.getHost() == null || url.getHost().length() == 0 ) {
			Util.logWarning(new WebUtil(), "Host is null in URL '" + urlString + "'");
			return null;
		}
				
		try {
			// GET
			HttpGet httpget = new HttpGet(url);
			HttpResponse response = httpclient.execute(httpget);
			Writer writer = new StringWriter();
			char[] buffer = new char[1024];
			Reader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
			int n;
			while ((n = reader.read(buffer)) != -1) {
				writer.write(buffer, 0, n);
			}
//			Util.logDebug(new WebUtil(), "HTML CONTENT " + writer.toString());
			return writer.toString();

		} catch (IOException e) {
	//kader temp only		Util.logWarning(new WebUtil(), "Failed to download URL '" + urlString + "'");
			e.printStackTrace();
			return null;
		} finally {
			httpclient.getConnectionManager().shutdown();
		}
	}
	
	public static String urlEncodeOrEmpty(String str) {
		try {
			return URLEncoder.encode(str, Util.UTF8.toString());
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return "";
		}
	}
	
  	public static String urlToFilename(final String inStr) {
		String str = inStr.replaceFirst("https?://", "");
		if(str.contains("www.aljazeera.net"))return  str; //kader
		String[] tokensRaw = str.split("/");
		ArrayList<String> tokensEncoded = new ArrayList<String>(tokensRaw.length);
		for( int i=0; i<tokensRaw.length; i++ ) {
			try {
				tokensEncoded.add(i, URLEncoder.encode(tokensRaw[i], Util.UTF8.toString()));
			} catch (UnsupportedEncodingException e) {
				tokensEncoded.add(i, UnsupportedEncodingException.class.getSimpleName());
			}
		}
		return StringUtils.join(tokensEncoded, "/");
	}

 	public static String urlToFilename1(final String inStr) {  		
		String str = inStr.replaceFirst("https?://", "");
		if(str.contains("www.aljazeera.net")){
			String[] tokensRaw = str.split("/");
			ArrayList<String> tokensEncoded = new ArrayList<String>(tokensRaw.length);
			for( int i=0; i<tokensRaw.length; i++ ) {
				try {
					tokensEncoded.add(i, URLDecoder.decode(tokensRaw[i], Util.UTF8.toString()));
				} catch (UnsupportedEncodingException e) {
					tokensEncoded.add(i, UnsupportedEncodingException.class.getSimpleName());	
				}
			}
			return StringUtils.join(tokensEncoded, "/");
		}
		String[] tokensRaw = str.split("/");
		ArrayList<String> tokensEncoded = new ArrayList<String>(tokensRaw.length);
		for( int i=0; i<tokensRaw.length; i++ ) {
			try {
				tokensEncoded.add(i, URLEncoder.encode(tokensRaw[i], Util.UTF8.toString()));
			} catch (UnsupportedEncodingException e) {
				tokensEncoded.add(i, UnsupportedEncodingException.class.getSimpleName());
			}
		}
		return StringUtils.join(tokensEncoded, "/");
	}


/*	public static String urlToFilename(final String inStr) {
		
		String str = inStr.replaceFirst("https?://", "");
		String[] tokensRaw = str.split("/");
		ArrayList<String> tokensEncoded = new ArrayList<String>(tokensRaw.length);
		for( int i=0; i<tokensRaw.length; i++ ) {
			tokensEncoded.add(i, tokensRaw[i]);
		}
		String res = StringUtils.join(tokensEncoded, "/");
		Util.logDebug(new WebUtil(), "^^^^^^^^^^^^^^^^^^^^^^^^^ " + res);
		return res; 
	}
*/
	public static String getStrippedNormalizedUrl(String url) {
		URI uri;
		try {
			String urlLowerCasedChomped = StringUtils.chomp(url, ";").toLowerCase(); 
			URL tmpURL = new URL(urlLowerCasedChomped);
			uri = new URI( tmpURL.getProtocol(), null, tmpURL .getHost(), tmpURL .getPort(), tmpURL.getPath(), null, null );
		} catch (URISyntaxException e) {
			return "uri-syntax-exception-at-VisitGetStrippedNormalizedUrl";
		} catch (MalformedURLException e) {
			return "malformed-url-exception-at-VisitGetStrippedNormalizedUrl";
		}
		return uri.normalize().toString();
	}
	
	public static boolean checkURLHostContains(String url, String needle) {
		// This implementation is more than 10x faster than using URL.getHost() according to this post:
		// http://stackoverflow.com/questions/4826061/what-is-the-fastest-way-to-get-the-domain-host-name-from-a-url
		int slashslash = url.indexOf("//") + 2;
		if( slashslash == 1 ) { // -1 + 2
			return false;
		}
		String domain = url.substring(slashslash, url.indexOf('/', slashslash));
		return domain.contains(needle);
	}
	
	public static String getTopPrivateDomain(String url) {
		URL parsedURL;
		try {
			parsedURL = new URL(url);

		} catch (MalformedURLException e) {
			return "malformed-url";
		}

		String uriHost = parsedURL.getHost();
		if( uriHost == null || uriHost.length() == 0 ) {
			return "empty-host";
		}
		InternetDomainName fullDomainName = InternetDomainName.from(uriHost);
		String topPrivateDomain = fullDomainName.topPrivateDomain().name();
		
		if( topPrivateDomain == null || topPrivateDomain.length() == 0 ) {
			return "empty-top-private-domain";
		} else {
			return topPrivateDomain;
		}
	}
}
