package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.pfsd.*;

/**
 * 
 * @author Kevin Burton 
 */
public class Config {

    public static String PFS_ROOT = "/tmp/peregrine-dfs";

    private static Membership membership = new Membership();

    /**
     * The current 'host' that we are running on.  This is used so that we can
     * determine whether we should local or remote readers/writers.  This should
     * be set correctly or we won't be able to tell that we are on the local
     * machine and would have performance issues though we should still perform
     * correctly.
     */
    public static Host host = null;
    
    public static void addPartitionMembership( int partition, List<Host> hosts ) {
        membership.setPartition( new Partition( partition ), hosts );
    }

    public static void addPartitionMembership( int partition, Host... hosts ) {

        List<Host> list = new ArrayList();

        for( Host host : hosts ) {
            list.add( host );
        }

        addPartitionMembership( partition, list );
        
    }
    
    public static void addPartitionMembership( int partition, String... hosts ) {

        List<Host> list = new ArrayList();

        for( int i = 0; i < hosts.length; ++i ) {
            String host = hosts[i];
            list.add( new Host( host, i, FSDaemon.PORT ) );
        }

        addPartitionMembership( partition, list );

    }

    public static Membership getPartitionMembership() {
        return membership;
    }

    public static Host getHost() {
        return host;
    }

    public static void setHost( Host _host ) {
        host = _host;
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