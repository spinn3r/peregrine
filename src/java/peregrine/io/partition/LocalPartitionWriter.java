package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class LocalPartitionWriter {

    /**
     * Chunk size for rollover files.
     */
    public static long CHUNK_SIZE = 134217728;
    
    private String path = null;

    private int chunk_id = 0;

    private Partition partition;

    private Host host;

    private PartitionWriterDelegate delegate = null;

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
            delegate.setAppend();
        else
            delegate.erase();
        
        //create the first chunk...
        delegate.rollover();
        
    }

    public void write( byte[] key_bytes, byte[] value_bytes )
        throws IOException {

        delegate.write( key_bytes, value_bytes );

        rolloverWhenNecessary();
        
    }

    public void close() throws IOException {
        //close the last opened partition...
        delegate.close();        
    }

    public String toString() {
        return path;
    }

    private void rolloverWhenNecessary() throws IOException {

        if ( delegate.chunkLength() > CHUNK_SIZE )
            delegate.rollover();
        
    }

}