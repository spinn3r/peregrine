package peregrine.config;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class PartitionRouteHistograph {

    private Map<Partition,AtomicInteger> partitionWriteHistograph = new ConcurrentHashMap();
    
    private AtomicInteger total = new AtomicInteger();
    
    public PartitionRouteHistograph( Config config ) {

        for( Partition partition : config.getMembership().getPartitions() ) {

            partitionWriteHistograph.put( partition, new AtomicInteger() );

        }
            
    }

    public void incr( Partition part ) {

        total.getAndIncrement();
        partitionWriteHistograph.get( part ).getAndIncrement();
        
     }
    
    public String toString() {
        return String.format( "total: %,d: %s", total.get(), partitionWriteHistograph.toString() );
    }

} 