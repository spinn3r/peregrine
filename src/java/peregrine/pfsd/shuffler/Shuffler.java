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

    private ExecutorService executors = null;

    public ShuffleOutputWriter writer = null;
    public ShuffleOutputWriter last = null;

    private int idx = 0;

    private String name;

    private Future future = null;

    private Config config = null;
    
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

            String path = config.getPFSPath( part, host, String.format( "/shuffle/%s-%s.tmp", name, idx++ ) );

            close();
            
            writer = new ShuffleOutputWriter( config, path );

        }

        writer.accept( from_partition, from_chunk, to_partition, data );
        
    }

    public void close() throws IOException {

        if ( this.future != null ) {

            try {
                this.future.get();
            } catch ( Exception e ) {
                throw new IOException( "Failed to close writer: " , e );
            }

        }

        last = writer;

        if ( last != null ) {
            // ok we have to close this now....
            this.future = executors.submit( new ShufflerCloseCallable( last ) );
        }

    }
    
}

class ShufflerCloseCallable implements Callable {

    private ShuffleOutputWriter writer;
    
    ShufflerCloseCallable( ShuffleOutputWriter writer ) {
        this.writer = writer;
    }

    @Override
    public Object call() throws Exception {

        // close this in a background task since this blocks.
        this.writer.close();
        
        return null;
        
    }
    
}