package peregrine.pfsd.shuffler;

import java.util.*;
import java.io.*;
import java.nio.*;

import peregrine.*;

/**
 */
public class OutputBuffer {

    /**
     * Chunk size for rollover files.
     */
    public static long COMMIT_SIZE = 134217728;

    /**
     * Individual buffer size for per partition writes.
     */
    public static int BUFFER_SIZE = 262144;

    /**
     * Blocks that are closed out and pending a write.  Keep these together with
     * the chunks the partitions they represent so that we can commit them on
     * disk as one unit.
     */
    private Map<Integer,List<byte[]>> closed = new HashMap();

    private Map<Integer,ByteBuffer> buffers = new HashMap();

    public OutputBuffer() {

        // create initial data structures so we don't have to worry about null
        // values.

        List<Partition> partitions = Config.getPartitionMembership().getPartitions( Config.getHost() );

        for ( Partition part : partitions ) {

            closed.put( part.getId(), new ArrayList() );
            buffers.put( part.getId(), newByteBuffer() );
            
        }
        
    }
    
    public void accept( int from_partition,
                        int from_chunk,
                        byte[] data ) {

        ByteBuffer buff = buffers.get( from_partition );

        if ( buff.position() + data.length > buff.capacity () ) {

            // this buffer is about to be full.. create another one

        }
        
    }

    public void close() throws IOException {

    }

    private ByteBuffer rollByteBuffer( ByteBuffer old, int partition ) {

        ByteBuffer result = newByteBuffer();

        synchronized( closed ) {

            synchronized( buffers ) {

            }

        }

        return result;
        
    }

    private ByteBuffer newByteBuffer() {
        return ByteBuffer.allocate( BUFFER_SIZE );
    }
    
}