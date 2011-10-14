package peregrine.pfsd.shuffler;

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
public class ShufflerFactory {

    private Map<String,Shuffler> instances = new HashMap();

    protected Config config;

    public long lastFlushed = -1;
    
    public ShufflerFactory( Config config ) {
        this.config = config;
    }
    
    public Shuffler getInstance( String name ) {

        Shuffler shuffler = instances.get( name );

        if ( shuffler == null ) {

            synchronized( instances ) {

                shuffler = instances.get( name );

                if ( shuffler == null ) {

                    shuffler = new Shuffler( config, name );
                    instances.put( name, shuffler );
                    
                } 

            }
            
        }

        return shuffler;
        
    }

    /**
     * Close all shufflers and flush their output to disk.
     */
    public void flush() throws IOException {

        for( Shuffler current : instances.values() ) {
            current.close();
        }

        lastFlushed = System.currentTimeMillis();
        
    }

    public long lastFlushed() {
        return lastFlushed();
    }
    
}