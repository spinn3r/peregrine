package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.partition.*;

import com.spinn3r.log5j.Logger;

/**
 * Take a given stream of input in the form of (K,V) and route it and write the
 * data to the correct partition.  If your data is already partitioned, with the correct
 * algorithm, at least right now you can run these in parallel
 */
public class ExtractWriter {

    private static final Logger log = Logger.getLogger();

    private List<PartitionWriter> output;

    private Config config;

    private PartitionRouteHistograph partitionWriteHistograph;
    
    public ExtractWriter( Config config, String path ) throws IOException {

        this.config = config;
        Membership membership = config.getMembership();
        
        output = new ArrayList( membership.size() );
        
        for( Partition partition : membership.getPartitions() ) {

            log.info( "Creating writer for partition: %s", partition );

            DefaultPartitionWriter writer = new DefaultPartitionWriter( config, partition, path );
            output.add( writer );
            
        }

        partitionWriteHistograph = new PartitionRouteHistograph( config );
        
    }

    public void write( Key key, Value value ) 
        throws IOException {

        write( key.toBytes(), value.toBytes() );
        
    }
    
    /**
     * If the Key is already a hashcode and we can route over it specify keyIsHashcode=true.
     */
    public void write( byte[] key, byte[] value )
        throws IOException {

        Partition partition = config.route( key );
        
        write( partition, key, value );
        
    }

    private void write( Partition part, byte[] key, byte[] value )
        throws IOException {

        partitionWriteHistograph.incr( part );
        
        output.get( part.getId() ).write( key, value );
        
    }

    public void close() throws IOException {

        for( PartitionWriter writer : output ) {
            writer.close();
        }

        log.info( "Partition write histograph: %s" , partitionWriteHistograph );
        
    }

}