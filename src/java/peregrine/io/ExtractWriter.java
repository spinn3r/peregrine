package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.partition.*;

/**
 * Take a given stream of input in the form of (K,V) and route it and write the
 * data to the correct partition.  If your data is already partitioned, with the correct
 * algorithm, at least right now you can run these in parallel
 */
public class ExtractWriter {

    private List<PartitionWriter> output;

    private String path;

    private int nr_partitions = -1;
    
    public ExtractWriter( Config config, String path ) throws IOException {

        this.path = path;

        Membership membership = config.getMembership();
        
        nr_partitions = membership.size();

        output = new ArrayList( nr_partitions );
        
        for( Partition partition : membership.getPartitions() ) {

            System.out.printf( "Creating writer for partition: %s\n", partition );

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

        Partition partition = Config.route( key, nr_partitions, keyIsHashcode );
        
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