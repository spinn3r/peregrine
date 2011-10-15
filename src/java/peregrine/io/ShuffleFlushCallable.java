package peregrine.io;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.net.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.partition.*;
import peregrine.keys.*;
import peregrine.map.*;
import peregrine.shuffle.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.chunk.*;
import peregrine.io.async.*;
import peregrine.pfs.*;
import peregrine.pfsd.shuffler.*;

import com.spinn3r.log5j.Logger;

import static peregrine.pfsd.FSPipelineFactory.*;

public class ShuffleFlushCallable implements Callable {

    private static final Logger log = Logger.getLogger();

    private ShuffleOutput output = null;

    private Config config = null;
    
    public ShuffleFlushCallable( Config config, ShuffleOutput output ) {
        this.config = config;
        this.output = output;
    }
    
    public Object call() throws Exception {

        // FIXME: dont allow us to close somethin out TWICE

        if ( output.flushing )
            return null;

        output.flushing = true;
        
        log.info( "Closing shuffle job output for chunk: %s", output.chunkRef );

        Map<Integer,ChannelBufferWritable> partitionOutput = getPartitionOutput();

        // now read the data and write it to all clients .. 

        int count = 0;

        // FIXME: ANY of these writes can fail and if they do we need to
        // continue and just gossip that they have failed...  this includes
        // write() AND close()

        System.out.printf( "FIXME9 We have %,d extendt\n", output.extents.size() );
        
        for( ShuffleOutputExtent extent : output.extents ) {

            System.out.printf( "FIXME13: working on extent %s... with count %s\n", extent, extent.count );
            
            ChannelBuffer buff = extent.buff;

            for ( int i = 0; i < extent.count; ++i ) {

                int to_partition = buff.readInt();
                int length       = buff.readInt();

                ChannelBuffer slice = buff.slice( buff.readerIndex() , length );

                ChannelBufferWritable client = partitionOutput.get( to_partition );

                if ( client == null )
                    throw new Exception( "NO client for partition: " + to_partition );
                
                client.write( slice );

                // bump up the writer index now for the next reader.
                buff.readerIndex( buff.readerIndex() + length );
                
                ++count;
                
            }

        }
        
        // now close all clients and we are done.
        
        for( ChannelBufferWritable client : partitionOutput.values() ) {
            client.close();
        }

        log.info( "Shuffled %,d entries.", count );

        return null;
        
    }

    private Map<Integer,ChannelBufferWritable> getPartitionOutput() {

        try {

            Map<Integer,ChannelBufferWritable> clients = new HashMap();

            Membership membership = config.getPartitionMembership();
            
            Set<Partition> partitions = membership.getPartitions();
            
            for( Partition part : partitions ) {

                List<Host> hosts = membership.getHosts( part );
                
                String path = String.format( "/%s/shuffle/%s/from-partition/%s/from-chunk/%s",
                                             part.getId(),
                                             output.name,
                                             output.chunkRef.partition.getId(),
                                             output.chunkRef.local );

                System.out.printf( "FIXME: hosts: %s, part: %s\n", hosts , part );
                
                ChannelBufferWritable client = new RemoteChunkWriterClient( hosts, path );
                client = new BufferedChannelBuffer( client , MAX_CHUNK_SIZE );
                
                clients.put( part.getId(), client );
                
            }

            return clients;
            
        } catch ( Exception e ) {
            // This should be ok as it will cause the map job to fail which will
            // then be caught by gossip.
            throw new RuntimeException( e );
        }

    }
    
}
