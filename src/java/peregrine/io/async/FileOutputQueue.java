
package peregrine.io.async;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * 
 */
public class FileOutputQueue {

    /**
     * How many messages to buffer before they go out to disk.
     */
    public int LIMIT = 1000;

    private String dest = null;

    private BlockingQueue<byte[]> queue = new LinkedBlockingDeque( LIMIT );

    private Future future = null;

    private boolean closed = false;
    
    public FileOutputQueue( String dest ) {

        this.dest = dest;

        FileOutputCallable callable = new FileOutputCallable( dest, queue );

        this.future = FileOutputService.submit( callable );
        
    }
    
    public void write( byte[] data ) throws IOException {

        try {

            if ( closed )
                throw new IOException( "closed" );
            
            queue.put( data );

        } catch ( Exception e ) {
            throw new IOException( e );
        }

    }

    public void close() throws IOException {

        closed = true;
        
        try {
            
            future.get();
            
        } catch ( Exception e ) {
            throw new IOException( e );
        }
        
    }
    
}

class FileOutputCallable implements Callable {

    private String dest;

    private BlockingQueue<byte[]> queue = null;

    FileOutputCallable( String dest , BlockingQueue<byte[]> queue ) {
        this.dest = dest;
        this.queue = queue;
    }
    
    public Object call() throws Exception {

        // for now write the file in place.  Checksums will allow us to detect
        // corruption (as will length) and we can resume upload if we wanted to
        // in the future and we supported merkle hash trees which we could
        // efficiently read off disk.

        File file = new File( dest );

        // always make parent directories for PUTs but do this in a dedicated
        // thread.
        new File( file.getParent() ).mkdirs();
        
        System.out.printf( "dest: %s\n" , dest );

        BufferedOutputStream out
            = new BufferedOutputStream( new FileOutputStream( file ) );
        
        while( true ) {

            // this blocks and that is ok because that's actually what we need.

            byte[] data = queue.take();

            if ( data.length == 0 )
                break;

            out.write( data );
            
        }

        out.close();

        return null;
        
    }
    
}