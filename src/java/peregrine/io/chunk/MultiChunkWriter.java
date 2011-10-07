package peregrine.io.chunk;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.io.async.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.io.*;

import com.spinn3r.log5j.*;

/**
 * Write each value to N chunk writers.
 */
public class MultiChunkWriter implements ChunkWriter {

    private static final Logger log = Logger.getLogger();

    protected List<ChunkWriter> delegates;
    
    public MultiChunkWriter( List<ChunkWriter> delegates ) throws IOException {

        if ( delegates == null || delegates.size() == 0 )
            throw new IOException( "No delegates" );

        this.delegates = delegates;
        
    }

    @Override
    public void write( final byte[] key, final byte[] value ) throws IOException {

        MultiChunkWriterIterator it = new MultiChunkWriterIterator( this ) {

                public void handle( ChunkWriter writer ) throws IOException {
                    writer.write( key, value );
                }

            };

        it.iterate();
        
    }

    @Override
    public void close() throws IOException {

        MultiChunkWriterIterator it = new MultiChunkWriterIterator( this ) {

                public void handle( ChunkWriter writer ) throws IOException {
                    writer.close();
                }

            };

        it.iterate();

    }

    @Override
    public int count() throws IOException {
        assertDelegates();
        return delegates.get(0).count();
    }

    @Override
    public long length() throws IOException {
        assertDelegates();
        return delegates.get(0).length();
    }

    /**
     * Determines how we handle failure of a specific ChunkWriter failing.  The
     * default is just to log the fact that we failed (which we should ALWAYS
     * do) but in production we should probably gossip about the failure with
     * the controller so that we understand what is happening.
     */
    public void handleFailure( Throwable cause ) {
        log.error( "Unable to handle chunk: " , cause );
    }

    protected void assertDelegates() throws IOException {

        if ( delegates.size() == 0 )
            throw new IOException( "No delegates available." );

    }

}

/**
 * Handles calling a method per ChunkWriter.
 */
abstract class MultiChunkWriterIterator {

    private MultiChunkWriter writer;
    
    public MultiChunkWriterIterator( MultiChunkWriter writer ) {
        this.writer = writer;
    }
    
    public void iterate() throws IOException {
    
        writer.assertDelegates();

        Iterator<ChunkWriter> it = writer.delegates.iterator();

        while( it.hasNext() ) {

            try {

                ChunkWriter current = it.next();

                handle( current );

            } catch ( Throwable t ) {

                it.remove();
                writer.handleFailure( t );

            }

        }

    }

    public abstract void handle( ChunkWriter writer ) throws IOException;
    
}