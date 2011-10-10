package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.chunk.*;
import peregrine.io.async.*;

import com.spinn3r.log5j.Logger;

/**
 * 
 */
public class NewPartitionWriter implements PartitionWriter {

    private static final Logger log = Logger.getLogger();

    /**
     * Chunk size for rollover files.
     */
    public static long CHUNK_SIZE = 134217728;

    protected String path;

    protected Partition partition;
    
    private int count = 0;

    private List<PartitionWriterDelegate> partitionWriterDelegates;

    private ChunkWriter chunkWriter = null;

    private int chunk_id = 0;

    /**
     * Bytes written.
     */
    private long written = 0;
    
    public NewPartitionWriter( Partition partition,
                               String path ) throws IOException {
        this( partition, path, false );
    }
    
    public NewPartitionWriter( Partition partition,
                               String path,
                               boolean append ) throws IOException {

        this.path = path;
        this.partition = partition;

        Membership partitionMembership = Config.getPartitionMembership();

        List<Host> hosts = partitionMembership.getHosts( partition );

        partitionWriterDelegates = new ArrayList();

        for( Host host : hosts ) {

            log.info( "Creating partition writer delegate for host: " + host );
            
            PartitionWriterDelegate delegate;

            System.out.printf( "FIXNE: host: %s vs %s\n", host, Config.getHost() );
            
            if ( host.equals( Config.getHost() ) ) {
                delegate = new LocalPartitionWriterDelegate();
            } else { 
                delegate = new RemotePartitionWriterDelegate();
            }

            delegate.init( partition, host, path );

            partitionWriterDelegates.add( delegate );

            if ( append ) 
                chunk_id = delegate.append();
            else
                delegate.erase();

        }

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

        closeChunkWriter();

        log.info( "Wrote %,d bytes to %s" , written, path );
        
    }

    @Override
    public String toString() {
        return path;
    }

    private void rolloverWhenNecessary() throws IOException {

        if ( chunkWriter.length() > CHUNK_SIZE )
            rollover();
        
    }

    private void closeChunkWriter() throws IOException {

        if ( chunkWriter != null ) {
            chunkWriter.close();
            written += chunkWriter.length();
        }

    }
    
    private void rollover() throws IOException {

        closeChunkWriter();
        
        List<OutputStream> outputStreams = new ArrayList();

        for ( PartitionWriterDelegate delegate : partitionWriterDelegates ) {
            outputStreams.add( delegate.newChunkWriter( chunk_id ) );
        }

        chunkWriter = new DefaultChunkWriter( new MultiOutputStream( outputStreams ) );
        
        ++chunk_id; // change the chunk ID now for the next file.

    }

}

