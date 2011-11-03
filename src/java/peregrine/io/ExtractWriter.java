package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.config.Config;
import peregrine.config.Membership;
import peregrine.config.Partition;
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

    private String path;

    private Config config;
    
    public ExtractWriter( Config config, String path ) throws IOException {

        this.config = config;
        this.path = path;

        Membership membership = config.getMembership();
        
        output = new ArrayList( membership.size() );
        
        for( Partition partition : membership.getPartitions() ) {

            log.info( "Creating writer for partition: %s", partition );

            DefaultPartitionWriter writer = new DefaultPartitionWriter( config, partition, path );
            output.add( writer );
            
        }
        
    }

    public void write( Key key, Value value )
        throws IOException {

        write( key, value, false );
        
    }

    public void write( byte[] key, byte[] value )
        throws IOException {

        write( key, value, false );
        
    }

    public void write( Key key, Value value, boolean keyIsHashcode ) 
        throws IOException {

        write( key.toBytes(), value.toBytes(), keyIsHashcode );
        
    }
    
    /**
     * If the Key is already a hashcode and we can route over it specify keyIsHashcode=true.
     */
    public void write( byte[] key, byte[] value, boolean keyIsHashcode )
        throws IOException {

        Partition partition = config.route( key, keyIsHashcode );
        
        write( partition, key, value );
        
    }

    private void write( Partition part, byte[] key, byte[] value )
        throws IOException {

        output.get( part.getId() ).write( key, value );
        
    }

    public void close() throws IOException {

        for( PartitionWriter writer : output ) {
            writer.close();
        }

    }

}