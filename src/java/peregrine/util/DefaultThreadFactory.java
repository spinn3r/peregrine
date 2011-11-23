package peregrine.util;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class DefaultThreadFactory implements ThreadFactory {

    public AtomicInteger idx = new AtomicInteger();;

    private String template;
    
    public DefaultThreadFactory( Class clazz ) {
        this( clazz.getName() );
    }

    public DefaultThreadFactory( String template ) {
        this.template = template;
    }

    public Thread newThread(Runnable r) {
        Thread thread = new Thread( r, template + ":" + idx.getAndIncrement() );
        thread.setDaemon( true );
        return thread;
    }
    
}