package peregrine.config;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class PartitionRouteHistograph {

    private Map<Partition,AtomicInteger> partitionWriteHistograph = new ConcurrentHashMap();
    
    private AtomicInteger total = new AtomicInteger();

    private Config config = null;
    
    public PartitionRouteHistograph( Config config ) {

        this.config = config;
        
        for( Partition partition : config.getMembership().getPartitions() ) {

            partitionWriteHistograph.put( partition, new AtomicInteger() );

        }
            
    }

    public void incr( Partition part ) {

        total.getAndIncrement();
        partitionWriteHistograph.get( part ).getAndIncrement();
        
     }
    
    public String toString() {

        StringBuilder hist = new StringBuilder();

        for( Partition part : config.getMembership().getPartitions() ) {

            if ( hist.length() > 0 )
                hist.append( ", " );

            hist.append( String.format( "%s=%s", part.getId(), partitionWriteHistograph.get( part ) ) );
            
        }
        
        return String.format( "total: %,d: %s", total.get(), hist.toString() );

    }

} 