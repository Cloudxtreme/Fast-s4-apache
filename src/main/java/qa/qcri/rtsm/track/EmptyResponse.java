package qa.qcri.rtsm.track;

import java.io.IOException;
import java.io.PrintStream;

import org.simpleframework.http.Request;
import org.simpleframework.http.Response;

/**
 * Sends an empty response from the tracker.
 * 
 * Based on log.php from the OWA framework, by Peter Adams
 *
 */
/**
 * @author chato
 * 
 */
public class EmptyResponse {
	private static byte[] GIF_1X1_DATA = { 71, 73, 70, 56, 55, 97, 1, 0, 1, 0,
			-128, 0, 0, -1, -1, -1, -1, -1, -1, 44, 0, 0, 0, 0, 1, 0, 1, 0, 0,
			2, 2, 68, 1, 0, 59, };

	public static void sendEmptyResponse(Request request, Response response) {
		try {
			if (request.getMethod().equals("POST")) {
				sendEmptyResponseForPOST(response);
			} else {
				sendEmptyResponseForGET(response);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void sendEmptyResponseForGET(Response response)
			throws IOException {
		response.set("Content-type", "image/gif");
		response.set("Content-length", 35);
		response.set("Connection", "close");
		response.set("Cache-Control",
				"private, no-cache, no-cache=Set-Cookie, proxy-revalidate");
		response.set("Expires", "Wed, 11 Jan 2000 12:59:00 GMT");
		response.set("Last-Modified", "Wed, 11 Jan 2006 12:59:00 GMT");
		response.set("Pragma", "no-cache");

		PrintStream body = response.getPrintStream();
		body.write(gif1x1()); // Very important: write(), do not print() !
		body.close();

		response.commit();
		response.close();
	}

	private static byte[] gif1x1() {
		return GIF_1X1_DATA;
	}

	private static void sendEmptyResponseForPOST(Response response)
			throws IOException {
		PrintStream body = response.getPrintStream();
		body.println(" ");
		response.close();
	}
}
