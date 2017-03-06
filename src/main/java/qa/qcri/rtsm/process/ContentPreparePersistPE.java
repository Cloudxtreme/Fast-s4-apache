package qa.qcri.rtsm.process;

import qa.qcri.rtsm.item.URLSeenParsed;
import qa.qcri.rtsm.persist.ContentPersister;
import qa.qcri.rtsm.util.Util;


public class ContentPreparePersistPE extends PersistedAbstractPE {
	
    private ContentPersister contentPersister;
    
	public void processEvent(URLSeenParsed urlSeenParsed) {
		Util.logDebug(this, "Got URLSeenParsed: " + urlSeenParsed.toString() );
    	contentPersister.set(urlSeenParsed);
    }

    @Override
    public void output() {
    	//No-op
    }
    
	@Override
	public void initInstance() {
		super.initInstance();
    	contentPersister = (ContentPersister)persister;
	}
}
