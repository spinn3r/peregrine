package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;

public class Config {

    public static long DFS_BLOCK_SIZE = 100000000;
    
    public static String DFS_ROOT = "/tmp/peregrine-dfs";

    private static Map<Partition,List<Host>> PARTITION_MEMBERSHIP = new HashMap();

    private static Map<String,Integer> HOST_ID_LOOKUP = new HashMap();

    private static int lookupHostId( String name ) {

        int id = -1;
        
        if ( HOST_ID_LOOKUP.containsKey( name ) ) {
            id = HOST_ID_LOOKUP.get( name );
        } else {
            id = HOST_ID_LOOKUP.size();
            HOST_ID_LOOKUP.put( name, id );
        } 

        return id;
        
    }
    
    public static void addPartitionMembership( int partition, String... hosts ) {

        List<Host> list = new ArrayList();

        for( int i = 0; i < hosts.length; ++i ) {

            String host = hosts[i];
            
            int id = lookupHostId( host );
            
            list.add( new Host( host, id, i ) );
            
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