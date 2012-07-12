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
package peregrine.shuffle.sender;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.http.*;
import peregrine.util.netty.*;
import peregrine.util.primitive.*;
import peregrine.io.chunk.*;

import com.spinn3r.log5j.Logger;

import static peregrine.worker.FSPipelineFactory.*;

public class ShuffleSender implements Flushable, Closeable {

    private static final Logger log = Logger.getLogger();

    private Config config = null;

    private Map<Integer,ShuffleOutputTarget> partitionOutput;

    private int count = 0;

    private String name;

    private ChunkReference chunkRef;

    private long length = 0;
    
    public ShuffleSender( Config config, String name, ChunkReference chunkRef ) {

        this.config = config;
        this.name = name;
        this.chunkRef = chunkRef;

        this.partitionOutput = getPartitionOutput();

    }

    public void emit( int to_partition, StructReader key, StructReader value ) throws ShuffleFailedException {

        ShuffleOutputTarget client = partitionOutput.get( to_partition );
        
        try {
        
            length += DefaultChunkWriter.write( client, key, value );

            ++client.count;
            ++count;

        } catch ( Exception e ) {

            config.getMembership().sendGossipToController( client.getHost(), e );

            // TODO: I think we have to block here until the controller tells us
            // what to do (in normal situations write to a new host).
            
            throw new ShuffleFailedException( String.format( "Unable to write to %s: %s" , client, e.getMessage() ) , e );

        }

    }

    public long length() {
        return length;
    }

    @Override
    public void flush() throws IOException {

        for( ShuffleOutputTarget client : partitionOutput.values() ) {
            client.flush();
        }

    }

    public void shutdown() throws IOException {

        for( ShuffleOutputTarget client : partitionOutput.values() ) {
            client.shutdown();
        }

    }
    
    @Override
    public void close() throws IOException {

        // flush the pending IO first 
        flush();
        
        // To make this faster we first call shutdown on all of them first which
        // closes them async. 
        shutdown();
        
        // now close the targets since they should all be shutdown.
        for( ShuffleOutputTarget client : partitionOutput.values() ) {
            client.close();
        }

    }
    
    private Map<Integer,ShuffleOutputTarget> getPartitionOutput() {

        try {

            Map<Integer,ShuffleOutputTarget> result = new HashMap();

            Membership membership = config.getMembership();
            
            Set<Partition> partitions = membership.getPartitions();
            
            for( Partition part : partitions ) {

                List<Host> hosts = membership.getHosts( part );

                if ( chunkRef == null )
                    throw new NullPointerException( "chunkRef" );

                if ( part == null )
                    throw new NullPointerException( "part" );

                String path = String.format( "/%s/shuffle/%s/from-partition/%s/from-chunk/%s",
                                             part.getId(),
                                             name,
                                             chunkRef.partition.getId(),
                                             chunkRef.local );

                HttpClient client = new HttpClient( config, hosts, path );

                ShuffleOutputTarget target
                    = new ShuffleOutputTarget( hosts.get( 0 ), client, MAX_CHUNK_SIZE - IntBytes.LENGTH  );

                result.put( part.getId(), target );
                
            }

            return result;
            
        } catch ( Exception e ) {
            // This should be ok as it will cause the map job to fail which will
            // then be caught by gossip.
            throw new RuntimeException( e );
        }

    }

    /**
     * The output for shuffle data.  
     */
    class ShuffleOutputTarget extends BufferedChannelBufferWritable {

        protected Host host;
        protected HttpClient client;

        /**
         * The number of entries written (count) to this target since the last flush.
         */
        protected int count = 0;
        
        public ShuffleOutputTarget( Host host, HttpClient client, int capacity ) {
            super( client, capacity - HttpClient.CHUNK_OVERHEAD );
            this.host = host;
            this.client = client;            
        }

        @Override
        public void preFlush() throws IOException {

            // add the number of entries written to this buffer (AKA the count)

            ChannelBuffer buff = ChannelBuffers.buffer( IntBytes.LENGTH ) ;
            buff.writeInt( count );
            
            this.buffers.add( buff );

            count = 0;
            
        }

        public Host getHost() {
            return host;
        }

        public HttpClient getClient() {
            return client;
        }
        
    }

}
