package peregrine.util;

import java.util.concurrent.*;

public class DefaultThreadFactory implements ThreadFactory {

    public int idx = 0;

    private Class template;
    
    public DefaultThreadFactory( Class clazz ) {
        this.template = clazz;
    }

    public Thread newThread(Runnable r) {
        Thread thread = new Thread( r, template.getName() + idx++ );
        thread.setDaemon( true );
        return thread;
    }
    
}