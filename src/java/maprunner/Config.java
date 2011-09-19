package maprunner;

import java.io.*;
import java.util.*;

import maprunner.util.*;

public class Config {

    public static long DFS_BLOCK_SIZE = 100000000;
    
    public static String DFS_ROOT = "/tmp/maprunner-dfs";

    private static Map<Partition,List<Host>> SHARD_MEMBERSHIP = new HashMap();

    public static void addShardMembership( int partition, String... hosts ) {

        List<Host> list = new ArrayList();

        for( int i = 0; i < hosts.length; ++i ) {
            list.add( new Host( hosts[i], i ) );
        }

        SHARD_MEMBERSHIP.put( new Partition( partition ), list );

    }

    public static Map<Partition,List<Host>> getShardMembership() {
        return SHARD_MEMBERSHIP;
    }

    public static String getDFSRoot( Host host ) {
        return String.format( "%s/%s" , Config.DFS_ROOT , host.getName() );
    }
        
    public static String getDFSPath( Host host, String path ) {
        return String.format( "%s%s" , getDFSRoot( host ), path );
    }

    /**
     * For a given key, in bytes, route it to the correct partition/shard.
     */
    public static int route( byte[] key_bytes,
                             int nr_shards,
                             boolean keyIsHashcode ) {

        int shard = -1;

        if ( keyIsHashcode ) {
            shard = (int)(Hashcode.toLong( key_bytes ) % nr_shards);
        } else { 
            shard = (int)(Hashcode.toLong( Hashcode.getHashcode( key_bytes ) ) % nr_shards);
        }

        return shard;
        
    }

}