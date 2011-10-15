package peregrine.pfsd.shuffler;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.async.*;

import com.spinn3r.log5j.Logger;

/**
 * Handles accepting shuffle data and rolling over shuffle files when buffers
 * are full.
 */
public class Shuffler {

    private static final Logger log = Logger.getLogger();

    private ExecutorService executors = null;

    public ShuffleOutputWriter writer = null;
    public ShuffleOutputWriter last = null;

    private int idx = 0;

    private String name;

    private Future future = null;

    private Config config = null;

    private int accepted = 0;
    
    public Shuffler( Config config, String name ) {

        this.name = name;
        this.config = config;
        
        ThreadFactory tf = new DefaultThreadFactory( getClass().getName() + "#" + name );
        
        executors = Executors.newCachedThreadPool( tf );

    }

    public void accept( int from_partition,
                        int from_chunk,
                        int to_partition,
                        byte[] data ) throws IOException {
        
        if ( writer == null || writer.length() > ShuffleOutputWriter.COMMIT_SIZE ) {

            Partition part = new Partition( to_partition );
            Host host = config.getHost();

            rollover();

            String path = config.getPFSPath( part, host, String.format( "/shuffle/%s/%010d.tmp", name, idx++ ) );

            writer = new ShuffleOutputWriter( config, path );

        }

        writer.accept( from_partition, from_chunk, to_partition, data );
        ++accepted;
        
    }

    public void rollover() throws IOException {

        if ( this.future != null ) {

            try {
                this.future.get();
            } catch ( Exception e ) {
                throw new IOException( "Failed to close writer: " , e );
            }

        }

        last = writer;

        if ( last != null ) {
            // ok we have to flush this to disk this now....
            this.future = executors.submit( new ShufflerFlushCallable( last ) );
        }

    }

    public void close() throws IOException {

        log.info( "Closing shuffler: %s", name );
        
        try {
            
            rollover();
            
            // block until we close
            if ( future != null )
                future.get();

            if ( accepted == 0 )
                log.warn( "Accepted no output for %s ", name );
            else 
                log.info( "Accepted %,d entries for %s ", accepted, name );
            
        } catch ( IOException e ) {
            throw e;
        } catch ( Exception e ) {
            throw new IOException( "Failed to close shufflers: " , e );
        }
        
    }

}

class ShufflerFlushCallable implements Callable {

    private ShuffleOutputWriter writer;
    
    ShufflerFlushCallable( ShuffleOutputWriter writer ) {
        this.writer = writer;
    }

    @Override
    public Object call() throws Exception {

        // close this in a background task since this blocks.
        this.writer.close();
        
        return null;
        
    }
    
}