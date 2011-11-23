package peregrine.io.async;

import java.io.*;
import java.util.*;

import peregrine.config.*;
import peregrine.http.*;

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

	// FIXME: I can dump the ENTIRE multi output stream if I also dump the 
	// entire local output writer work and ALWAYS use pipeline writes which will
	// both simplify the code and make it more maintainable.
	
    private static final Logger log = Logger.getLogger();

    protected Map<Host,OutputStream> delegates;
    
    public MultiOutputStream( Map<Host,OutputStream> delegates ) throws IOException {

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

    public void shutdown() throws IOException {

        MultiOutputStreamIterator it = new MultiOutputStreamIterator( this ) {

                public void handle( OutputStream writer ) throws IOException {

                    if ( writer instanceof HttpClient ) {
                        HttpClient client = (HttpClient)writer;
                        client.shutdown();
                    }

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
    public void handleFailure( OutputStream out, Host host, Throwable cause ) {
    	
        log.error( String.format( "Unable to handle chunk on host %s for %s", host, out) , cause );
        
        // FIXME: we need to gossip about this because a host may be down and 
        // failing writes
        
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

        Set<Map.Entry<Host,OutputStream>> set = writer.delegates.entrySet();
        
        Iterator<Map.Entry<Host,OutputStream>> it = set.iterator();

        while( it.hasNext() ) {

        	Map.Entry<Host,OutputStream> entry = it.next();

            OutputStream current = entry.getValue();

            try {

                handle( current );

            } catch ( Throwable t ) {

                it.remove();
                writer.handleFailure( current, entry.getKey(), t );

            }

        }

    }

    public abstract void handle( OutputStream out ) throws IOException;
    
}