package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.partition.*;
import peregrine.values.*;

import com.spinn3r.log5j.Logger;

/**
 * Take a given stream of input in the form of (K,V) and route it and write the
 * data to the correct partition.  If your data is already partitioned, with the correct
 * algorithm, at least right now you can run these in parallel
 */
public class ExtractWriter {

    private static final Logger log = Logger.getLogger();

    private List<DefaultPartitionWriter> output;

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
    
    /**
     * If the Key is already a hashcode and we can route over it specify keyIsHashcode=true.
     */
    public void write( StructReader key, StructReader value )
        throws IOException {

        Partition partition = config.route( key.toByteArray() );
        
        write( partition, key, value );
        
    }

    private void write( Partition part, StructReader key, StructReader value )
        throws IOException {

        partitionWriteHistograph.incr( part );
        
        output.get( part.getId() ).write( key, value );
        
    }

    public long length() {

        long result = 0;
        
        for( PartitionWriter writer : output ) {
            result += writer.length();
        }

        return result;
        
    }
    
    public void close() throws IOException {

        //TODO: not sure why but this made it slower.
        for( PartitionWriter writer : output ) {
            writer.shutdown();
        }
        
        for( PartitionWriter writer : output ) {
            writer.close();
        }

        log.info( "Partition write histograph: %s" , partitionWriteHistograph );
        
    }

}