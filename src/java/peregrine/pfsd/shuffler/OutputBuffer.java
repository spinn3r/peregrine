package peregrine.pfsd.shuffler;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.async.*;

/**
 */
public class OutputBuffer {

    /**
     * Chunk size for rollover files.
     */
    public static long COMMIT_SIZE = 134217728;

    /**
     * Real world HTTP chunks size we would see.
     */
    public static int HTTP_CHUNK_SIZE = 3000;

    private AtomicInteger ptr = new AtomicInteger();

    private ShufflePacket[] index;

    private String path;
    
    public OutputBuffer( String path ) {

        this.index = new ShufflePacket[ (int)(COMMIT_SIZE / HTTP_CHUNK_SIZE) ];
        this.path = path;
        
    }
    
    public void accept( int from_partition,
                        int from_chunk,
                        byte[] data ) {

        ShufflePacket pack = new ShufflePacket( from_partition, from_chunk, data );

        index[ ptr.getAndIncrement() ] = pack;

    }

    public void close() throws IOException {

        // we are done working with this buffer.  serialize it to disk now and
        // close it out.

        List<Partition> partitions = Config.getPartitionMembership().getPartitions( Config.getHost() );

        Map<Partition,List<ShufflePacket>> lookup = new HashMap();
        
        for( Partition part : partitions ) {
            lookup.put( part, new ArrayList() );
        }

        for( int i = 0; i < index.length; ++i ) {

            ShufflePacket current = index[i];

            if ( current == null )
                continue;

            if ( i > ptr.get() )
                break;

            lookup.get( current.partition ).add( current );
            
        }

        // now stream these out to disk...

        AsyncOutputStream out = new AsyncOutputStream( path );
        
        for( Map.Entry<Partition,List<ShufflePacket>> entry : lookup.entrySet() ) {

            Partition part = entry.getKey();
            List<ShufflePacket> packets = entry.getValue();
            
            out.write( LongBytes.toByteArray( part.getId() ) );

            for( ShufflePacket pack : packets ) {

                out.write( pack.data );
                
            }
            
        }

        out.close();
        
    }
    
}

class ShufflePacket {

    public int partition;
    public int chunk;
    public byte[] data; 

    public ShufflePacket( int from_partition,
                          int from_chunk,
                          byte[] data ) {

        this.partition = from_partition;
        this.chunk = from_chunk;
        this.data = data;

    }

}