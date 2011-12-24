package peregrine.shuffle.receiver;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.config.*;
import peregrine.io.util.*;

import com.spinn3r.log5j.Logger;

/**
 * Handles accepting shuffle data and rolling over shuffle files when buffers
 * are full.
 */
public class ShuffleReceiverFactory {

    private static final Logger log = Logger.getLogger();
    
    private ConcurrentHashMap<String,ShuffleReceiver> instances = new ConcurrentHashMap();

    protected Config config;

    public long lastFlushed = -1;
    
    public ShuffleReceiverFactory( Config config ) {
        this.config = config;
    }
    
    public ShuffleReceiver getInstance( String name ) {

        ShuffleReceiver result = instances.get( name );

        // double check idiom.  this doesn't happen very often so this should be
        // fine and won't impact performance.
        if ( result == null ) {

            synchronized( instances ) {

                result = instances.get( name );

                if ( result == null ) {

                    result = new ShuffleReceiver( config, name );
                    instances.putIfAbsent( name, result );
                    result = instances.get( name );
                    
                } 

            }
            
        }

        return result;
        
    }

    /**
     * Close all shufflers and flush their output to disk.
     */
    public void flush() throws IOException {

        log.info( "Flushing %,d shufflers...", instances.size() );

        for( ShuffleReceiver current : instances.values() ) {
            current.close();
        }

        lastFlushed = System.currentTimeMillis();

        log.info( "Flushing %,d shufflers...done", instances.size() );

        // now throw the current instances away because we can't use them any
        // more and this will also free up memory.
        instances = new ConcurrentHashMap();
            
    }

    public void purge( String name ) throws IOException {

        // only done so that we can benchmark the performance of certain
        // algorithms.
        
        if ( config.getPurgeShuffleData() == false )
            return;
        
        String dir = config.getShuffleDir( name );
        
        log.info( "Purging shuffler directory: %s", dir );

        Files.remove( dir );
        
    }
    
    public long lastFlushed() {
        return lastFlushed();
    }
    
}