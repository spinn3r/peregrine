package peregrine;

import java.io.*;
import java.util.*;

import peregrine.util.*;
import peregrine.pfsd.*;

import com.spinn3r.log5j.Logger;

/**
 *
 * 
 */
public class Config {
    
    private static final Logger log = Logger.getLogger();

    /**
     * Default port for serving requests.
     */
    public static int DEFAULT_PORT = 11112;

    public static int DEFAULT_CONCURRENCY = 1;
    
    /**
     * Default root dir for serving files.
     */
    public static String DEFAULT_ROOT = "/tmp/peregrine-fs";

    /**
     * The root for storing data.
     */
    public String root = DEFAULT_ROOT;

    /**
     * Partition membership.
     */
    protected Membership membership = new Membership();

    /**
     * The current 'host' that we are running on.  This is used so that we can
     * determine whether we should local or remote readers/writers.  This should
     * be set correctly or we won't be able to tell that we are on the local
     * machine and would have performance issues though we should still perform
     * correctly.
     */
    protected Host host = null;

    /**
     * The controller coordinating job tasks in the cluster.  
     */
    protected Host controller = null;

    /**
     * Unique index of hosts. 
     */
    protected Set<Host> hosts = new HashSet();

    /**
     * The number of partitions per host.
     */
    protected int partitions_per_host;

    /**
     * The number of replicas per file we are configured for.  Usually 2 or 3.
     */
    protected int replicas;

    /**
     * The concurrency on a per host basis.  How many mappers and reducers each
     * can run.
     */
    protected int concurrency = DEFAULT_CONCURRENCY;
    
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
            list.add( new Host( host, i, DEFAULT_PORT ) );
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

    public boolean isController() {
        return this.controller.getName().equals( getHost().getName() );
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

    public int getPartitionsPerHost() {
        return partitions_per_host;
    }

    public void setPartitionsPerHost( int partitions_per_host ) {
        this.partitions_per_host = partitions_per_host;
    }

    public int getReplicas() { 
        return this.replicas;
    }

    public void setReplicas( int replicas ) { 
        this.replicas = replicas;
    }

    public int getConcurrency() { 
        return this.concurrency;
    }

    public void setConcurrency( int concurrency ) { 
        this.concurrency = concurrency;
    }

    public String getRoot( Partition partition ) {
        return String.format( "%s/%s" , root , partition.getId() );
    }
        
    public String getPath( Partition partition, String path ) {
        return String.format( "%s%s" , getRoot( partition ), path );
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
     * Load the given configuration.
     */
    public static Config load( String[] args ) throws IOException {

        String conf = "conf/peregrine.conf";
        String hosts = "conf/peregrine.hosts";

        // we should probably convert to getopts for this.... 
        for ( String arg : args ) {

            if ( arg.startsWith( "-c=" ) ) {
                conf = arg.split( "=" )[1];
                continue;
            }

            if ( arg.startsWith( "-h=" ) ) {
                hosts = arg.split( "=" )[1];
                continue;
            }

        }
        
        return load( conf, hosts );

    }

    public static Config load( String conf, String hosts ) throws IOException {
        return load( new File( conf ), new File( hosts ) );
    }

    /**
     * Parse a config file from disk.
     */
    public static Config load( File conf_file, File hosts_file ) throws IOException {

        Properties props = new Properties();
        props.load( new FileInputStream( conf_file ) );

        StructMap struct = new StructMap( props );
        
        String root        = struct.get( "root" );
        int port           = struct.getInt( "port" );

        String hostname = System.getenv( "HOSTNAME" );

        if ( hostname == null )
            hostname = "localhost";

        if ( port <= 0 )
            port = DEFAULT_PORT;
        
        Config config = new Config();

        config.setRoot( root );
        config.setHost( new Host( hostname, port ) );
        config.setController( Host.parse( struct.get( "controller" ) ) );

        config.setPartitionsPerHost( struct.getInt( "partitions_per_host" ) );
        config.setReplicas( struct.getInt( "replicas" ) );
        config.setConcurrency( struct.getInt( "concurrency" ) );
        
        // now read the hosts file...
        List<Host> hosts = readHosts( hosts_file );

        PartitionLayoutEngine engine = new PartitionLayoutEngine( config, hosts );
        engine.build();

        Membership membership = engine.toMembership();

        config.membership = membership;
        config.hosts.addAll( hosts );

        if ( ! config.hosts.contains( config.getHost() ) &&
             ! config.isController() ) {
            throw new IOException( "Host is not define in hosts file nor is it the controller: " + config.getHost() );
        }
        
        log.info( "Using controller: %s", config.getController() );
        log.info( "Running with partition layout: \n%s", membership.toMatrix() );

        return config;
        
    }

    public static List<Host> readHosts( File file ) throws IOException {

        FileInputStream fis = new FileInputStream( file );

        byte[] data = new byte[ (int)file.length() ];
        fis.read( data );

        String[] lines = new String( data ).split( "\n" );

        List<Host> hosts = new ArrayList();

        for( String line : lines ) {

            line = line.trim();
            
            if ( "".equals( line ) )
                continue;

            if ( line.startsWith( "#" ) )
                continue;

            String hostname = line;
            int port = DEFAULT_PORT;
            
            if ( line.contains( ":" ) ) {

                String[] split = line.split( ":" );

                hostname = split[0];
                port     = Integer.parseInt( split[1] );

            }

            Host host = new Host( hostname, port );
            hosts.add( host);
            
        }

        return hosts;
        
    }

}