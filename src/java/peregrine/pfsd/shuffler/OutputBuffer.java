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
 */
public class OutputBuffer {

    private static final Logger log = Logger.getLogger();

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

        log.info( "Going to write shuffle for %s", partitions );
        
        if ( partitions == null || partitions.size() == 0 )
            throw new IOException( "No partitions defined for: " + Config.getHost() );
        
        Map<Integer,List<ShufflePacket>> lookup = new HashMap();
        
        for( Partition part : partitions ) {

            System.out.printf( "FIXME: Adding lookup for : %s \n" , part );
            lookup.put( part.getId(), new ArrayList() );

        }

        for( int i = 0; i < index.length; ++i ) {

            ShufflePacket current = index[i];

            if ( current == null )
                continue;

            if ( i > ptr.get() )
                break;

            List<ShufflePacket> packets = lookup.get( current.partition );

            if ( packets == null )
                throw new IOException( "No locally defined partition for: " + current.partition );
            
            packets.add( current );
            
        }

        // now stream these out to disk...

        AsyncOutputStream out = new AsyncOutputStream( path );
        
        for( Map.Entry<Integer,List<ShufflePacket>> entry : lookup.entrySet() ) {

            int part = entry.getKey();
            List<ShufflePacket> packets = entry.getValue();
            
            out.write( LongBytes.toByteArray( part ) );

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