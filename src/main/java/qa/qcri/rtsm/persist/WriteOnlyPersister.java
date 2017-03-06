package qa.qcri.rtsm.persist;

import io.s4.persist.Persister;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class WriteOnlyPersister implements Persister {
	
	public int persistCount;
	
    @Override
    public int cleanOutGarbage() throws InterruptedException {
        return 0;
    }

    @Override
    public Object get(String arg0) throws InterruptedException {
        return null;
    }

    @Override
    public Map<String, Object> getBulk(String[] arg0)
            throws InterruptedException {
        return new HashMap<String, Object>();
    }

    @Override
    public Map<String, Object> getBulkObjects(String[] arg0)
            throws InterruptedException {
        return new HashMap<String, Object>();
    }

    @Override
    public int getCacheEntryCount() {
        return 1;
    }

    @Override
    public Object getObject(String arg0) throws InterruptedException {
        return null;
    }

    @Override
    public int getPersistCount() {
        return persistCount;
    }

    @Override
    public int getQueueSize() {
        return 0;
    }

    @Override
    public Set<String> keySet() {
        return new HashSet<String>();
    }

    @Override
    public void remove(String arg0) throws InterruptedException {

    }
    
    @Override
    public void setAsynch(String key, Object value, int persistTime) {
        try {
            set(key, value, persistTime);
        } catch (InterruptedException ie) {
        }
    }
}
