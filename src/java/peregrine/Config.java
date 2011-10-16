package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.pfsd.*;

/**
 *
 * 
 * @author Kevin Burton 
 */
public class Config {

    public static String DEFAULT_ROOT = "/tmp/peregrine-dfs";

    /**
     * The root for storing data.
     */
    public String root = DEFAULT_ROOT;

    /**
     * Partition membership.
     */
    private Membership membership = new Membership();

    /**
     * The current 'host' that we are running on.  This is used so that we can
     * determine whether we should local or remote readers/writers.  This should
     * be set correctly or we won't be able to tell that we are on the local
     * machine and would have performance issues though we should still perform
     * correctly.
     */
    public Host host = null;

    /**
     * The controller coordinating job tasks in the cluster.  
     */
    public Host controller = null;
    
    public Set<Host> hosts = new HashSet();
    
    public Config() { }

    public Config( String host, int port ) {
        this( new Host( host, port ) );
    }

    public Config( Host host ) {
        setHost( host );
        setRoot( String.format( "%s/%s/%s", DEFAULT_ROOT, host.getName(), host.getPort() ) );
    }
    
    public void addMembership( int partition, List<Host> hosts ) {

        membership.setPartition( new Partition( partition ), hosts );

        // make sure we have a hosts entry for every host.
        for( Host host : hosts ) {
            this.hosts.add( host );
        }
        
    }

    public void addMembership( int partition, Host... hosts ) {

        List<Host> list = new ArrayList();

        for( Host host : hosts ) {
            list.add( host );
        }

        addMembership( partition, list );
        
    }
    
    public void addMembership( int partition, String... hosts ) {

        List<Host> list = new ArrayList();

        for( int i = 0; i < hosts.length; ++i ) {
            String host = hosts[i];
            list.add( new Host( host, i, FSDaemon.PORT ) );
        }

        addMembership( partition, list );

    }

    public Membership getMembership() {
        return membership;
    }

    /**
     */
    public Host getHost() {
        return host;
    }

    public Config setHost( Host host ) {
        this.host = host;
        return this;
    }

    public Host getController() {
        return controller;
    }

    public Config setController( Host controller ) {
        this.controller = controller;
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
    public Partition route( byte[] key_bytes,
                            boolean keyIsHashcode ) {

        int nr_partitions = membership.size();
        
        // TODO: we should to build out an entirely new router which we can
        // replace with a custom partitioning system if the user decides that
        // there may be a smarter partitioning system they can use.
        
        // TODO: we only need a FEW bytes to route a key , not the WHOLE thing
        // if it is a hashcode.  For example... we can route to 255 partitions
        // with just one byte... that IS if it is a hashode.  with just TWO
        // bytes we can route to 65536 partitions which is probably fine for all
        // users for a LONG time.
        
        int partition = -1;

        if ( keyIsHashcode ) {

            long value = Math.abs( LongBytes.toLong( key_bytes ) );
            partition = (int)(value % nr_partitions);
            
        } else { 

            long value = Math.abs( LongBytes.toLong( Hashcode.getHashcode( key_bytes ) ) );
            partition = (int)( value % nr_partitions);

        }

        return new Partition( partition );
        
    }

    /**
     * Parse a config file from disk.
     */
    public static Config parse( File file ) throws IOException {

        Properties props = new Properties();
        props.load( new FileInputStream( file ) );

        String root        = props.get( "root" ).toString();
        int port           = Integer.parseInt( props.get( "port" ).toString() );
        String controller  = props.get( "controller" ).toString();

        String hostname = System.getenv( "HOSTNAME" );

        if ( hostname == null )
            hostname = "localhost";

        Config config = new Config();

        config.setRoot( root );
        config.setHost( new Host( hostname, port ) );
        config.setController( Host.parse( controller ) );

        return config;
        
    }

}