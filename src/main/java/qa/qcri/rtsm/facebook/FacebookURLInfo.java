package qa.qcri.rtsm.facebook;

import com.restfb.Facebook;

public class FacebookURLInfo {
	@Facebook
	protected long like_count;
	
	@Facebook
	protected long share_count;

	public long getLike_count() {
		return like_count;
	}

	public long getShare_count() {
		return share_count;
	}
}