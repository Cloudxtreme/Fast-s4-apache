package qa.qcri.rtsm.process;

import io.s4.processor.AbstractPE;

import org.apache.log4j.Level;

import qa.qcri.rtsm.util.Util;

public abstract class LoggableAbstractPE extends AbstractPE {
	
	private String logLevel;
	
	public String getLogLevel() {
		return logLevel;
	}

	public void setLogLevel(String loglevelStr) {
		Level level = Level.toLevel(loglevelStr);
		if( ! level.toString().equals(loglevelStr.toString())) {
			throw new IllegalArgumentException("Could not find loglevel '" + loglevelStr + "', available: " + Level.TRACE + ", " + Level.DEBUG + ", " + Level.INFO);
		}
		Util.logInfo(this, "Setting log level to " + level);
		Util.setLogLevel(this, level );
		this.logLevel = loglevelStr;
	}
}
