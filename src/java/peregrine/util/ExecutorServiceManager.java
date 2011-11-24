package peregrine.util;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import com.spinn3r.log5j.Logger;

public class ExecutorServiceManager {

    private static final Logger log = Logger.getLogger();

    private Map<Class,ExecutorService> executorServices = new ConcurrentHashMap();

    public ExecutorService getExecutorService( Class clazz ) {

        ExecutorService result;

        result = executorServices.get( clazz );

        // double check idiom
        if ( result == null ) {

            synchronized( this ) {

                result = executorServices.get( clazz );
                
                if ( result == null ) {

                    result = Executors.newCachedThreadPool( new DefaultThreadFactory( clazz ) );
                    executorServices.put( clazz, result );
                    
                }
                
            }
            
        }

        return result;
        
    }

    public void shutdownAndAwaitTermination() {

        for( Class clazz : executorServices.keySet() ) {

            log.info( "Shutting down executor service: %s", clazz.getName() );

            try {
                
                ExecutorService current = executorServices.get( clazz );
                current.shutdown();
                current.awaitTermination( Long.MAX_VALUE, TimeUnit.MILLISECONDS );
                
            } catch ( InterruptedException e ) {
                throw new RuntimeException( e );
            }

        }

    }
    
}
