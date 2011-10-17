package peregrine.shuffle.receiver;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.async.*;

import com.spinn3r.log5j.Logger;

/**
 * Handles accepting shuffle data and rolling over shuffle files when buffers
 * are full.
 */
public class ShuffleReceiverFactory {

    private static final Logger log = Logger.getLogger();

    private Map<String,ShuffleReceiver> instances = new HashMap();

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
                    instances.put( name, result );
                    
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
        instances = new HashMap();
            
    }

    public long lastFlushed() {
        return lastFlushed();
    }
    
}