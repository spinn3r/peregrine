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
package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.http.*;
import peregrine.io.chunk.*;
import peregrine.os.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.worker.*;

import com.spinn3r.log5j.Logger;

/**
 * Write a partition as a given stream of key/value pairs and rollover chunks as
 * they are filled.
 */
public class DefaultPartitionWriter implements PartitionWriter, ChunkWriter {

    private static final Logger log = Logger.getLogger();

    private static final boolean ENABLE_LOCAL_DELEGATE = false;

    protected String path;

    protected Partition partition;
    
    private List<PartitionWriterDelegate> partitionWriterDelegates;

    private ChunkWriter chunkWriter = null;

    private int chunkId = 0;

    /**
     * Total length of this file (bytes written) to this partition writer..
     */
    private long length = 0;

    private Config config;
    
    public DefaultPartitionWriter( Config config,
                                   Partition partition,
                                   String path ) throws IOException {
        this( config, partition, path, false );
    }

    public DefaultPartitionWriter( Config config,
                                   Partition partition,
                                   String path,
                                   boolean append ) throws IOException {

        // TODO:
        //
        // https://bitbucket.org/burtonator/peregrine/issue/177/defaultpartitionwriter-doesnt-order-hosts
        //
        // Right now we just get ALL hosts with no special priority or ordering for throughput.
        //
        // We need make sure we first contact the closest host first by route.
        
        this( config, partition, path, append, config.getMembership().getHosts( partition ) );
        
    }

    public DefaultPartitionWriter( Config config,
                                   Partition partition,
                                   String path,
                                   boolean append,
                                   List<Host> hosts ) throws IOException {

        this( config, partition, path, append, hosts, MappedFileWriter.DEFAULT_AUTO_SYNC );

    }

    public DefaultPartitionWriter( Config config,
                                   Partition partition,
                                   String path,
                                   boolean append,
                                   List<Host> hosts,
                                   boolean autoSync ) throws IOException {

        this.config = config;
        this.partition = partition;
        this.path = path;

        partitionWriterDelegates = new ArrayList();

        for( Host host : hosts ) {

            log.info( "Creating partition writer delegate for host: " + host );

            PartitionWriterDelegate delegate;
            
            if ( ENABLE_LOCAL_DELEGATE && host.equals( config.getHost() ) ) {
                delegate = new LocalPartitionWriterDelegate( config,  autoSync );
            } else { 
                delegate = new RemotePartitionWriterDelegate( config, autoSync );
            }

            delegate.init( config, partition, host, path );

            partitionWriterDelegates.add( delegate );

            if ( append ) 
                chunkId = delegate.append();
            else
                delegate.erase();

        }

        //create the first chunk...
        rollover();
        
    }

    @Override
    public void write( StructReader key, StructReader value )
        throws IOException {

        Hashcode.assertKeyLength( key );

        chunkWriter.write( key, value );

        rolloverWhenNecessary();
        
    }

    @Override
    public void flush() throws IOException {
    
        if ( chunkWriter != null ) {
            chunkWriter.flush();
            length += chunkWriter.length();
        }

    }

    @Override
    public void close() throws IOException {

        closeChunkWriter();

        log.info( "Wrote %,d bytes to %s on %s" , length, path , partition );
        
    }

    @Override
    public void shutdown() throws IOException {

        if ( chunkWriter != null )
            chunkWriter.shutdown();
        
    }
    
    @Override
    public String toString() {
        return path;
    }

    @Override
    public long length() {
        return length;
    }
    
    private void rolloverWhenNecessary() throws IOException {

        if ( chunkWriter.length() > config.getChunkSize() )
            rollover();
        
    }

    private void closeChunkWriter() throws IOException {

        if ( chunkWriter != null ) {
            chunkWriter.close();
            length += chunkWriter.length();
        }

    }
    
    private void rollover() throws IOException {

        closeChunkWriter();
        
        Map<Host,ChannelBufferWritable> writablesPerHost = new HashMap();

        HttpClient client = null;

        String pipeline = "";
        
        for ( PartitionWriterDelegate delegate : partitionWriterDelegates ) {

            Host local = config.getHost();
            
            if ( delegate.getHost().equals( local ) ) {

                ChannelBufferWritable writable = delegate.newChunkWriter( chunkId );

                writablesPerHost.put( delegate.getHost(), writable );
                
            } else if ( client == null ) {

                ChannelBufferWritable writable = delegate.newChunkWriter( chunkId );
                
                client = (HttpClient)writable;
                writablesPerHost.put( delegate.getHost(), writable );

            } else {
                pipeline += delegate.getHost() + " ";
            }

        }

        if ( client != null ) {

            if ( pipeline != null && pipeline.trim().length() > 0 ) {
                log.info( "Going to pipeline requests to: '%s'", pipeline );
                client.setHeader( FSHandler.X_PIPELINE_HEADER, pipeline );
            }

        }
        
        chunkWriter = new DefaultChunkWriter( config, new MultiChannelBufferWritable( writablesPerHost ) );
        
        ++chunkId; // change the chunk ID now for the next file.

    }

}

