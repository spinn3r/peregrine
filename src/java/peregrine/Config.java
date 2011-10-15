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

    public static String DEFAULT_ROOT = "/tmp/peregrine-dfs";
    
    public String root = DEFAULT_ROOT;

    private Membership membership = new Membership();

    /**
     * The current 'host' that we are running on.  This is used so that we can
     * determine whether we should local or remote readers/writers.  This should
     * be set correctly or we won't be able to tell that we are on the local
     * machine and would have performance issues though we should still perform
     * correctly.
     */
    public Host host = null;

    public Set<Host> hosts = new HashSet();
    
    public Config() { }

    public Config( String host, int port ) {
        setHost( new Host( host, port ) );
        setRoot( String.format( "%s/%s/%s", DEFAULT_ROOT, host, port ) );
    }

    public void addPartitionMembership( int partition, List<Host> hosts ) {

        membership.setPartition( new Partition( partition ), hosts );

        // make sure we have a hosts entry for every host.
        for( Host host : hosts ) {
            this.hosts.add( host );
        }
        
    }

    public void addPartitionMembership( int partition, Host... hosts ) {

        List<Host> list = new ArrayList();

        for( Host host : hosts ) {
            list.add( host );
        }

        addPartitionMembership( partition, list );
        
    }
    
    public void addPartitionMembership( int partition, String... hosts ) {

        List<Host> list = new ArrayList();

        for( int i = 0; i < hosts.length; ++i ) {
            String host = hosts[i];
            list.add( new Host( host, i, FSDaemon.PORT ) );
        }

        addPartitionMembership( partition, list );

    }

    public Membership getPartitionMembership() {
        return membership;
    }

    public Host getHost() {
        return host;
    }

    public Config setHost( Host _host ) {
        host = _host;
        return this;
    }

    public Set<Host> getHosts() {
        return hosts;
    }
    
    public String getRoot() {
        return root;
    }

    public Config setRoot( String root ) {
        this.root = root;
        return this;
    }
    
    public String getPFSRoot( Partition partition, Host host ) {
        return String.format( "%s/%s" , root , partition.getId() );
    }
        
    public String getPFSPath( Partition partition, Host host, String path ) {
        return String.format( "%s%s" , getPFSRoot( partition, host ), path );
    }

    public String getShuffleDir( String name ) {
        return String.format( "%s/tmp/shuffle/%s", getRoot(), name);
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