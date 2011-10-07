
package peregrine.io.async;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * 
 */
public abstract class BaseAscyncOutputStream extends BaseOutputStream {

    /**
     * How many messages to buffer before they go out to disk.  The best bet
     * here is to put this behind a BufferedOutputStream so you're writing in
     * 16k blocks so this would in theory buffer 1.6MB.
     */
    public static int LIMIT = 100;

    private static final byte[] EOF = new byte[0];
    
    private BlockingQueue<byte[]> queue = new LinkedBlockingDeque( LIMIT );

    private Future future = null;

    private boolean closed = false;

    public void init( Future future ) {
        this.future = future;
    }
    
    public BlockingQueue<byte[]> getQueue() {
        return queue;
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
