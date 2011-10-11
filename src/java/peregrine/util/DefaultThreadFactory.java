package peregrine.util;

import java.util.concurrent.*;

public class DefaultThreadFactory implements ThreadFactory {

    public int idx = 0;

    private String template;
    
    public DefaultThreadFactory( Class clazz ) {
        this( clazz.getName() );
    }

    public DefaultThreadFactory( String template ) {
        this.template = template;
    }

    public Thread newThread(Runnable r) {
        Thread thread = new Thread( r, template + idx++ );
        thread.setDaemon( true );
        return thread;
    }
    
}