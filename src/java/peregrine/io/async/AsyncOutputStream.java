
package peregrine.io.async;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * 
 */
public class AsyncOutputStream extends OutputStream {

    /**
     * How many messages to buffer before they go out to disk.  The best bet
     * here is to put this behind a BufferedOutputStream so you're writing in
     * 16k blocks so this would in theory buffer 1.6MB.
     */
    public static int LIMIT = 100;

    private static final byte[] EOF = new byte[0];
    
    private String dest = null;

    private BlockingQueue<byte[]> queue = new LinkedBlockingDeque( LIMIT );

    private Future future = null;

    private boolean closed = false;
    
    public AsyncOutputStream( String dest ) {

        this.dest = dest;

        AsyncOutputStreamCallable callable = new AsyncOutputStreamCallable( dest, queue );

        this.future = AsyncOutputStreamService.submit( callable );
        
    }
    
    public void write( byte[] data ) throws IOException {

        try {

            if ( closed )
                throw new IOException( "closed" );

            // don't allow this to be written directly as this is the way we
            // close the async callable (with a zero byte array).

            if ( data.length == 0 )
                return;
            
            queue.put( data );

        } catch ( Exception e ) {
            throw new IOException( e );
        }

    }

    public void write( byte[] b, int off, int len ) throws IOException {

        byte[] data = new byte[ len ];

        System.arraycopy( b, off, data, 0, len );

        write( data );
        
    }

    public void write( int b ) throws IOException {
        write( new byte[] { (byte)b } );
    }
    
    public void flush() throws IOException {

    }
    
    public void close() throws IOException {

        closed = true;
        
        try {
            
            queue.put( EOF );            

            future.get();
            
        } catch ( Exception e ) {
            throw new IOException( e );
        }
        
    }
    
}
