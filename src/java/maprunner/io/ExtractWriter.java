package maprunner.io;

import java.io.*;
import java.util.*;

import maprunner.*;
import maprunner.util.*;

/**
 * Take a given stream of input in the form of (K,V) and route it and write the
 * data to the correct partition.  If your data is already partitioned, with the correct
 * algorithm, at least right now you can run these in parallel
 */
public class ExtractWriter {

    private Object[] PARTITION_OUTPUT = null;

    private String path = null;

    private VarintWriter varintWriter = new VarintWriter();

    private int nr_partitions = -1;
    
    public ExtractWriter( String path ) throws IOException {

        this.path = path;

        Map<Partition,List<Host>> partitionMembership = Config.getPartitionMembership();
        
        nr_partitions = partitionMembership.size();

        PARTITION_OUTPUT = new Object[nr_partitions];

        for( Partition partition : partitionMembership.keySet() ) {

            List<Host> membership = partitionMembership.get( partition );

            System.out.printf( "Creating writer for partition: %s (%s)\n", partition, membership );

            List<LocalPartitionWriter> output = new ArrayList();
            
            for ( Host member : membership ) {
                output.add( new LocalPartitionWriter( Config.getDFSPath( partition, member, path ) ) );
            }

            PARTITION_OUTPUT[partition.getId()] = output;
            
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

        byte[] key_bytes   = key.toBytes();
        byte[] value_bytes = value.toBytes();

        write( key_bytes, value_bytes, keyIsHashcode );
        
    }
    
    /**
     * If the Key is already a hashcode and we can route over it specify keyIsHashcode=true.
     */
    public void write( byte[] key_bytes, byte[] value_bytes, boolean keyIsHashcode )
        throws IOException {

        Partition partition = Config.route( key_bytes, nr_partitions, keyIsHashcode );
        
        write( partition, key_bytes, value_bytes );
        
    }

    private void write( Partition partition,
                        byte[] key_bytes,
                        byte[] value_bytes )
        throws IOException {

        List<LocalPartitionWriter> output = (List<LocalPartitionWriter>) PARTITION_OUTPUT[partition.getId()];

        //FIXME: the distributed version should parallel dispatch these and
        //write to the partitions directly.

        for( LocalPartitionWriter out : output ) {
            out.write( key_bytes, value_bytes );
        }
        
    }

    public void close() throws IOException {

        for (int i = 0; i < PARTITION_OUTPUT.length; ++i ) {

            List<LocalPartitionWriter> output = (List<LocalPartitionWriter>) PARTITION_OUTPUT[i];
            
            for( LocalPartitionWriter out : output ) {
                System.out.printf( "Closing: %s\n", out );
                out.close();
            }

        }

    }

}