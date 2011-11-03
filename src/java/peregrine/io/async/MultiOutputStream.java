package peregrine.io.async;

import java.io.*;
import java.util.*;
import com.spinn3r.log5j.*;

/**
 * Write each value to N output streams.  All of these streams are async and
 * only block when their buffer is full.
 *
 * In the future investigate using scatter/gather with async IO for performance
 * reasons.
 * 
 */
public class MultiOutputStream extends BaseOutputStream {

    private static final Logger log = Logger.getLogger();

    protected List<OutputStream> delegates;
    
    public MultiOutputStream( List<OutputStream> delegates ) throws IOException {

        if ( delegates == null || delegates.size() == 0 )
            throw new IOException( "No delegates" );

        this.delegates = delegates;
        
    }

    @Override
    public void write( final byte[] value ) throws IOException {

        MultiOutputStreamIterator it = new MultiOutputStreamIterator( this ) {

                public void handle( OutputStream out ) throws IOException {
                    out.write( value );
                }

            };

        it.iterate();
        
    }

    @Override
    public void close() throws IOException {

        MultiOutputStreamIterator it = new MultiOutputStreamIterator( this ) {

                public void handle( OutputStream writer ) throws IOException {
                    writer.close();
                }

            };

        it.iterate();

    }

    @Override
    public void flush() throws IOException {

        MultiOutputStreamIterator it = new MultiOutputStreamIterator( this ) {

                public void handle( OutputStream writer ) throws IOException {
                    writer.close();
                }

            };

        it.iterate();

    }

    /**
     * Determines how we handle failure of a specific OutputStream failing.  The
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
 * Handles calling a method per OutputStream.
 */
abstract class MultiOutputStreamIterator {

    private MultiOutputStream writer;
    
    public MultiOutputStreamIterator( MultiOutputStream writer ) {
        this.writer = writer;
    }
    
    public void iterate() throws IOException {
    
        writer.assertDelegates();

        Iterator<OutputStream> it = writer.delegates.iterator();

        while( it.hasNext() ) {

            try {

                OutputStream current = it.next();

                handle( current );

            } catch ( Throwable t ) {

                it.remove();
                writer.handleFailure( t );

            }

        }

    }

    public abstract void handle( OutputStream out ) throws IOException;
    
}