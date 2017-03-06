package qa.qcri.rtsm.process;

import io.s4.persist.Persister;

public abstract class PersistedAbstractPE extends LoggableAbstractPE {
	
	protected Persister persister;
	
	protected int persistTime;

	public Persister getPersister() {
		return persister;
	}

	public void setPersister(Persister persister) {
		this.persister = persister;
	}

	public int getPersistTime() {
		return persistTime;
	}

	public void setPersistTime(int persistTime) {
		this.persistTime = persistTime;
	}

}
