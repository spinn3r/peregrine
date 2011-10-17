package peregrine.shuffle.sender;

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
import peregrine.reduce.*;
import peregrine.util.*;
import peregrine.values.*;
import peregrine.io.chunk.*;
import peregrine.io.async.*;
import peregrine.pfs.*;

import com.spinn3r.log5j.Logger;

import static peregrine.pfsd.FSPipelineFactory.*;

public class ShuffleSenderFlushCallable implements Callable {

    private static final Logger log = Logger.getLogger();

    private ShuffleSenderBuffer output = null;

    private Config config = null;
    
    public ShuffleSenderFlushCallable( Config config, ShuffleSenderBuffer output ) {
        this.config = config;
        this.output = output;
    }
    
    public Object call() throws Exception {

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
        
        for( ShuffleSenderExtent extent : output.extents ) {
            
            ChannelBuffer buff = extent.buff;

            for ( int i = 0; i < extent.emits; ++i ) {

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

            Membership membership = config.getMembership();
            
            Set<Partition> partitions = membership.getPartitions();
            
            for( Partition part : partitions ) {

                List<Host> hosts = membership.getHosts( part );

                String path = String.format( "/%s/shuffle/%s/from-partition/%s/from-chunk/%s/count/%s",
                                             part.getId(),
                                             output.name,
                                             output.chunkRef.partition.getId(),
                                             output.chunkRef.local,
                                             output.partitionCount.get( part.getId() ) );

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
