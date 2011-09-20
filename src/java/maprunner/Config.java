package maprunner;

import java.io.*;
import java.util.*;

import maprunner.util.*;

public class Config {

    public static long DFS_BLOCK_SIZE = 100000000;
    
    public static String DFS_ROOT = "/tmp/maprunner-dfs";

    private static Map<Partition,List<Host>> PARTITION_MEMBERSHIP = new HashMap();

    public static void addPartitionMembership( int partition, String... hosts ) {

        List<Host> list = new ArrayList();

        for( int i = 0; i < hosts.length; ++i ) {
            list.add( new Host( hosts[i], i ) );
        }

        PARTITION_MEMBERSHIP.put( new Partition( partition ), list );

    }

    public static Map<Partition,List<Host>> getPartitionMembership() {
        return PARTITION_MEMBERSHIP;
    }

    public static String getDFSRoot( Partition partition, Host host ) {
        return String.format( "%s/%s/%s" , Config.DFS_ROOT , host.getName(), partition.getId() );
    }
        
    public static String getDFSPath( Partition partition, Host host, String path ) {
        return String.format( "%s%s" , getDFSRoot( partition, host ), path );
    }

    /**
     * For a given key, in bytes, route it to the correct partition/partition.
     */
    public static Partition route( byte[] key_bytes,
                                   int nr_partitions,
                                   boolean keyIsHashcode ) {

        int partition = -1;

        if ( keyIsHashcode ) {

            long value = Hashcode.toLong( key_bytes );
            
            partition = Math.abs( (int)(value % nr_partitions) );

        } else { 

            long value = Hashcode.toLong( Hashcode.getHashcode( key_bytes ) );
            partition = Math.abs( (int)( value % nr_partitions) );

        }

        return new Partition( partition );
        
    }

}