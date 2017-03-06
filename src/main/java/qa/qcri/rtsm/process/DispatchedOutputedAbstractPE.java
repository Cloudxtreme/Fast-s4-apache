package qa.qcri.rtsm.process;

import io.s4.dispatcher.EventDispatcher;

public abstract class DispatchedOutputedAbstractPE extends LoggableAbstractPE {
	
    protected EventDispatcher dispatcher;
    protected String outputStreamName;

    public EventDispatcher getDispatcher() {
        return dispatcher;
    }

    public void setDispatcher(EventDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public String getOutputStreamName() {
        return outputStreamName;
    }

    public void setOutputStreamName(String outputStreamName) {
        this.outputStreamName = outputStreamName;
    }
}
