
package peregrine.io.async;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class AsyncOutputStreamCallable implements Callable {

    private String dest;

    private BlockingQueue<byte[]> queue = null;

    AsyncOutputStreamCallable( String dest , BlockingQueue<byte[]> queue ) {
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

        BufferedOutputStream out
            = new BufferedOutputStream( new FileOutputStream( file ) );

        try { 

            while( true ) {

                // this blocks and that is ok because that's actually what we need.

                byte[] data = queue.take();

                if ( data.length == 0 ) {
                    break;
                }

                out.write( data );
                
            }

        } finally {
            out.close();
        }

        return null;
        
    }
    
}