package de.quoss.mq.client;

public class MqClientException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    
    public MqClientException() {
        super();
    }
    
    public MqClientException(final String s) {
        super(s);
    }
    
    public MqClientException(final Throwable t) {
        super(t);
    }
    
    public MqClientException(final String s, final Throwable t) {
        super(s, t);
    }
    
}
