package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.config.Config;
import peregrine.config.Host;
import peregrine.config.Membership;
import peregrine.config.Partition;
import peregrine.http.*;
import peregrine.io.chunk.*;
import peregrine.io.async.*;
import peregrine.pfsd.*;

import com.spinn3r.log5j.Logger;

/**
 * 
 */
public class DefaultPartitionWriter implements PartitionWriter {

    private static final Logger log = Logger.getLogger();

    /**
     * Chunk size for rollover files.
     */
    public static long CHUNK_SIZE = (long) Math.pow(2, 27); // 128MB

    protected String path;

    protected Partition partition;
    
    private List<PartitionWriterDelegate> partitionWriterDelegates;

    private ChunkWriter chunkWriter = null;

    private int chunk_id = 0;

    /**
     * Total lengh of this file (bytes written) to this partition writer..
     */
    private long length = 0;

    private Config config;
    
    public DefaultPartitionWriter( Config config,
                                   Partition partition,
                                   String path ) throws IOException {
        this( config, partition, path, false );
    }
    
    public DefaultPartitionWriter( Config config,
                                   Partition partition,
                                   String path,
                                   boolean append ) throws IOException {

        this.config = config;
        this.partition = partition;
        this.path = path;

        Membership membership = config.getMembership();

        // TODO/FIXME: we need make sure we first contact the closest host first by route.

        List<Host> hosts = membership.getHosts( partition );

        partitionWriterDelegates = new ArrayList();

        for( Host host : hosts ) {

            log.info( "Creating partition writer delegate for host: " + host );
            
            PartitionWriterDelegate delegate;

            if ( host.equals( config.getHost() ) ) {
                delegate = new LocalPartitionWriterDelegate();
            } else { 
                delegate = new RemotePartitionWriterDelegate();
            }

            delegate.init( config, partition, host, path );

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

        log.info( "Wrote %,d bytes to %s on %s" , length, path , partition );
        
    }

    @Override
    public void shutdown() throws IOException {

        if ( chunkWriter != null )
            chunkWriter.shutdown();
        
    }
    
    @Override
    public String toString() {
        return path;
    }

    @Override
    public long length() {
        return length;
    }
    
    private void rolloverWhenNecessary() throws IOException {

        if ( chunkWriter.length() > CHUNK_SIZE )
            rollover();
        
    }

    private void closeChunkWriter() throws IOException {

        if ( chunkWriter != null ) {
            chunkWriter.close();
            length += chunkWriter.length();
        }

    }
    
    private void rollover() throws IOException {

        closeChunkWriter();
        
        Map<Host,OutputStream> outputStreams = new HashMap();

        HttpClient client = null;

        String pipeline = "";
        
        for ( PartitionWriterDelegate delegate : partitionWriterDelegates ) {

            Host local = config.getHost();
            
            if ( delegate.getHost().equals( local ) ) {

                OutputStream out = delegate.newChunkWriter( chunk_id );

                outputStreams.put( delegate.getHost(), out );
                
            } else if ( client == null ) {

                OutputStream out = delegate.newChunkWriter( chunk_id );
                
                client = (HttpClient)out;
                outputStreams.put( delegate.getHost(), client );

            } else {
                pipeline += delegate.getHost() + " ";
            }

        }

        if ( client != null ) {
            log.info( "Going to pipeline requests to: %s", pipeline );
            client.setHeader( FSHandler.X_PIPELINE_HEADER, pipeline );
        }
        
        chunkWriter = new DefaultChunkWriter( new MultiOutputStream( outputStreams ) );
        
        ++chunk_id; // change the chunk ID now for the next file.

    }

}

