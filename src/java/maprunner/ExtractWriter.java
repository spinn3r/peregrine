package maprunner;

import java.io.*;
import java.util.*;

import maprunner.util.*;

/**
 * Take a given stream of input in the form of (K,V) and route it and write the
 * data to the correct partition.  If your data is already partitioned, with the correct
 * algorithm, at least right now you can run these in parallel
 */
public class ExtractWriter {

    private Object[] PARTITIONS = null;
    private Object[] PARTITION_OUTPUT = null;

    private String path = null;

    private VarintWriter varintWriter = new VarintWriter();

    private int nr_partitions = -1;
    
    public ExtractWriter( String path ) throws IOException {

        this.path = path;

        Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();
        
        int nr_partitions = partitionMembership.size();

        PARTITIONS       = new Object[nr_partitions];
        PARTITION_OUTPUT = new Object[nr_partitions];

        int i = 0; 
        for( Partition partition : partitionMembership.keySet() ) {

            System.out.printf( "part: %s\n", partition );

            List<Host> membership = partitionMembership.get( partition );

            List<PartitionWriter> output = new ArrayList();
            
            for ( Host member : membership ) {

                output.add( new PartitionWriter( Config.getDFSPath( partition, member, path ) ) );
                
            }

            PARTITION_OUTPUT[i] = output;
            PARTITIONS[i]       = membership;

        }
        
    }

    public void write( Key key, Value value )
        throws IOException {

        write( key, value, false );
        
    }

    /**
     * If the Key is already a hashcode and we can route over it specify
     * keyIsHashcode=true.
     */
    public void write( Key key, Value value, boolean keyIsHashcode )
        throws IOException {

        byte[] key_bytes   = key.toBytes();
        byte[] value_bytes = value.toBytes();

        Partition partition = Config.route( key_bytes, nr_partitions, keyIsHashcode );

        write( partition, key_bytes, value_bytes );
        
    }

    private void write( Partition partition, byte[] key_bytes, byte[] value_bytes )
        throws IOException {

        List<PartitionWriter> output = (List<PartitionWriter>) PARTITION_OUTPUT[partition.id];

        for( PartitionWriter out : output ) {
            out.write( key_bytes, value_bytes );
        }
        
    }

    public void close() throws IOException {

        for (int i = 0; i < PARTITION_OUTPUT.length; ++i ) {

            List<PartitionWriter> output = (List<PartitionWriter>) PARTITION_OUTPUT[i];

            if ( output == null )
                continue;
            
            for( PartitionWriter out : output ) {
                out.close();
            }

        }

    }

}