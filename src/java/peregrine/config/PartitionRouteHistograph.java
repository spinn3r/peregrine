package peregrine.config;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Maintains an index of where keys are stored to analyze placement.
 */
public class PartitionRouteHistograph {

    private AtomicInteger total = new AtomicInteger();

    private Config config = null;

    private AtomicInteger[] data;
    
    public PartitionRouteHistograph( Config config ) {

        this.config = config;

        data = new AtomicInteger[ config.getMembership().getPartitions().size() ];
        
        for( int i = 0; i < data.length; ++i ) {
            data[i] = new AtomicInteger();
        }

    }

    public void incr( Partition part ) {

        total.getAndIncrement();
        data[ part.getId() ].getAndIncrement();
        
     }
    
    public String toString() {

        StringBuilder hist = new StringBuilder();

        for( Partition part : config.getMembership().getPartitions() ) {

            if ( hist.length() > 0 )
                hist.append( ", " );

            hist.append( String.format( "%s=%s", part.getId(), data[ part.getId() ] ) );
            
        }
        
        return String.format( "total: %,d: %s", total.get(), hist.toString() );

    }

} 