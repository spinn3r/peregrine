package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;
import peregrine.io.chunk.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class LocalPartitionWriter implements PartitionWriter {

    /**
     * Chunk size for rollover files.
     */
    public static long CHUNK_SIZE = 134217728;
    
    private String path = null;

    private int chunk_id = 0;

    private Partition partition;

    private Host host;

    private PartitionWriterDelegate delegate = null;

    private ChunkWriter chunkWriter = null;

    public LocalPartitionWriter( Partition partition,
                                 Host host,
                                 String path ) throws IOException {

        this( partition, host, path, false );

    }
        
    public LocalPartitionWriter( Partition partition,
                                 Host host,
                                 String path,
                                 boolean append ) throws IOException {

        delegate = new LocalPartitionWriterDelegate();
        delegate.init( partition, host, path );

        if ( append ) 
            chunk_id = delegate.append();
        else
            delegate.erase();
        
        //create the first chunk...
        rollover();
        
    }

    @Override
    public void write( byte[] key_bytes, byte[] value_bytes )
        throws IOException {

        chunkWriter.write( key_bytes, value_bytes );

        rolloverWhenNecessary();
        
    }

    @Override
    public void close() throws IOException {
        //close the last opened chunk writer...
        chunkWriter.close();        
    }

    @Override
    public String toString() {
        return path;
    }

    private void rolloverWhenNecessary() throws IOException {

        if ( chunkWriter.length() > CHUNK_SIZE )
            rollover();
        
    }

    private void rollover() throws IOException {

        if ( chunkWriter != null )
            chunkWriter.close();

        chunkWriter = new DefaultChunkWriter( delegate.newChunkWriter( chunk_id ) );
        
        ++chunk_id; // change the chunk ID now for the next file.
        
    }

}