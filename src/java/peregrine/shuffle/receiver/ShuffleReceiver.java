/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.shuffle.receiver;

import java.io.*;
import java.util.concurrent.*;
import peregrine.util.*;
import peregrine.config.Config;
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

    private volatile ShuffleOutputWriter writer = null;

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

                    if ( writer != null ) {
                        log.info( "Rolling over %s " , writer );
                    }

                    ShuffleOutputWriter last = writer;

                    String path = String.format( "%s/%010d.tmp", config.getShuffleDir( name ), idx++ );
                    
                    writer = new ShuffleOutputWriter( config, path );

                    // ok... now writes should be done to writer... 
                    rollover( last );

                }

            }

        }

        writer.accept( from_partition, from_chunk, to_partition, count, data );
        ++accepted;
        
    }

    private boolean needsRollover() {
        return writer == null || writer.hasCapacity() == false;
    }
    
    private void rollover( ShuffleOutputWriter writer ) throws IOException {

        if ( writer == null )
            return;
        
        // Make sure the previous one was complete.
        if ( this.future != null ) {
            
            try {
                this.future.get();
            } catch ( Exception e ) {
                throw new IOException( "Failed to close writer: " , e );
            }
            
        }
                
        // ok we have to flush this to disk this now....
        this.future = executors.submit( new ShuffleReceiverFlushCallable( writer ) );

    }

    public void close() throws IOException {

        log.info( "Closing shuffler: %s", name );
        
        try {
            
            rollover( writer );
            
            // block until we close
            if ( future != null )
                future.get();

            if ( accepted == 0 )
                log.warn( "Accepted no output for %s ", name );
            else 
                log.info( "Accepted %,d entries for %s ", accepted, name );

            executors.shutdown();
            
        } catch ( IOException e ) {
            throw e;
        } catch ( Exception e ) {
            throw new IOException( "Failed to close shufflers: " , e );
        }
        
    }

    public String toString() {
        return String.format( "shuffle:%s" , name );
    }
    
}
