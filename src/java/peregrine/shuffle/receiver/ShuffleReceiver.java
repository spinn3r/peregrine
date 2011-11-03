package peregrine.shuffle.receiver;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.config.Config;
import peregrine.io.async.*;
import peregrine.shuffle.*;

import org.jboss.netty.buffer.*;

import com.spinn3r.log5j.Logger;

/**
 * Handles accepting shuffle data and rolling over shuffle files when buffers
 * are full.
 */
public class ShuffleReceiver {

    private static final Logger log = Logger.getLogger();

    private ExecutorService executors = null;

    public ShuffleOutputWriter writer = null;
    public ShuffleOutputWriter last = null;

    private int idx = 0;

    private String name;

    private Future future = null;

    private Config config = null;

    private int accepted = 0;

    public ShuffleReceiver( Config config, String name ) {

        this.name = name;
        this.config = config;
        
        ThreadFactory tf = new DefaultThreadFactory( getClass().getName() + "#" + name );
        
        executors = Executors.newCachedThreadPool( tf );

    }

    public void accept( int from_partition,
                        int from_chunk,
                        int to_partition,
                        int count,
                        ChannelBuffer data ) throws IOException {

        if ( needsRollover() ) {

            // this must be synchronous on rollover or some other way to handle
            // data loss as we would quickly create a number of smaller
            // ShuffleReceiverOutputWriters.

            synchronized( this ) {

                if ( needsRollover() ) {

                    log.info( "Rolling over %s " , writer );
                    
                    rollover();

                    String path = String.format( "%s/%010d.tmp", config.getShuffleDir( name ), idx++ );
                    
                    writer = new ShuffleOutputWriter( config, path );

                }

            }

        }

        writer.accept( from_partition, from_chunk, to_partition, count, data );
        ++accepted;
        
    }

    private boolean needsRollover() {
        return writer == null || writer.hasCapacity() == false;
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
            this.future = executors.submit( new ShuffleReceiverFlushCallable( last ) );
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
