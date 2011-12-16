package peregrine.shuffle.sender;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;

import peregrine.config.*;
import peregrine.http.*;
import peregrine.util.netty.*;
import peregrine.util.primitive.*;
import peregrine.io.chunk.*;

import com.spinn3r.log5j.Logger;

import static peregrine.pfsd.FSPipelineFactory.*;
import peregrine.values.*;

public class ShuffleSender {

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
    
    public void close() throws IOException {

        // now close all clients and we are done.

        for( ShuffleOutputTarget client : partitionOutput.values() ) {
            client.flush();
            client.getClient().shutdown();
        }
        
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

                String path = String.format( "/%s/shuffle/%s/from-partition/%s/from-chunk/%s",
                                             part.getId(),
                                             name,
                                             chunkRef.partition.getId(),
                                             chunkRef.local );

                HttpClient client = new HttpClient( hosts, path );

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

        private Host host;
        private HttpClient client;
        
        public ShuffleOutputTarget( Host host, HttpClient client, int capacity ) {
            super( client, capacity - HttpClient.CHUNK_OVERHEAD );
            this.host = host;
            this.client = client;            
        }

        @Override
        public void preFlush() throws IOException {

            // add the number of entries written to this buffer 
            byte[] data = IntBytes.toByteArray( this.buffers.size() );

            this.buffers.add( ChannelBuffers.wrappedBuffer( data ) );
            
        }

        public Host getHost() {
            return host;
        }

        public HttpClient getClient() {
            return client;
        }
        
    }

}
