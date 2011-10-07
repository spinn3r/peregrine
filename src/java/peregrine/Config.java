package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;

public class Config {

    public static String PFS_ROOT = "/tmp/peregrine-dfs";

    private static Membership membership = new Membership();

    public static void addPartitionMembership( int partition, List<Host> hosts ) {
        membership.setPartition( new Partition( partition ), hosts );
    }

    public static void addPartitionMembership( int partition, String... hosts ) {

        List<Host> list = new ArrayList();

        for( int i = 0; i < hosts.length; ++i ) {

            String host = hosts[i];

            list.add( new Host( host, i ) );
            
        }

        membership.setPartition( new Partition( partition ), list );

    }

    public static Membership getPartitionMembership() {
        return membership;
    }

    public static String getPFSRoot( Partition partition, Host host ) {
        return String.format( "%s/%s/%s" , Config.PFS_ROOT , host.getName(), partition.getId() );
    }
        
    public static String getPFSPath( Partition partition, Host host, String path ) {
        return String.format( "%s%s" , getPFSRoot( partition, host ), path );
    }

    /**
     * For a given key, in bytes, route it to the correct partition/partition.
     */
    public static Partition route( byte[] key_bytes,
                                   int nr_partitions,
                                   boolean keyIsHashcode ) {

        //FIXME: we need to build out an entirely new router which we can
        //replace with a custom partitioning system.
        
        //TODO: we only need a FEW bytes to route a key , not the WHOLE thing if
        //it is a hashcode.  For example... we can route to 255 partitions with
        //just one byte... that IS if it is a hashode.  with just TWO bytes we
        //can route to 65536 partitions which is probably fine for all users for
        //a LONG time.
        
        int partition = -1;

        if ( keyIsHashcode ) {

            long value = Math.abs( LongBytes.toLong( key_bytes ) );
            partition = (int)(value % nr_partitions);
            
        } else { 

            long value = Math.abs( LongBytes.toLong( Hashcode.getHashcode( key_bytes ) ) );
            partition = (int)( value % nr_partitions);

        }
        
        //FIXME: read these from a cached list by lookup for performance reasons
        //(GC and new object creation).  It just seems ugly to create these for
        //every route.
        
        return new Partition( partition );
        
    }

}