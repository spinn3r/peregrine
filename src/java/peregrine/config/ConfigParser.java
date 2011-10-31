package peregrine.config;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.pfsd.*;

import com.spinn3r.log5j.Logger;

public class ConfigParser {

    private static final Logger log = Logger.getLogger();

    /**
     * Load the given configuration.
     */
    public static Config parse( String[] args ) throws IOException {

        String conf  = "conf/peregrine.conf";
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
        
        return parse( conf, hosts );

    }

    public static Config parse( String conf, String hosts ) throws IOException {
        return parse( new File( conf ), new File( hosts ) );
    }

    /**
     * Parse a config file from disk.
     */
    public static Config parse( File conf_file, File hosts_file ) throws IOException {

        Properties props = new Properties();
        props.load( new FileInputStream( conf_file ) );

        StructMap struct = new StructMap( props );
        
        String root        = struct.get( "root" );
        int port           = struct.getInt( "port" );

        String hostname = System.getenv( "HOSTNAME" );

        if ( hostname == null )
            hostname = "localhost";

        if ( port <= 0 )
            port = Config.DEFAULT_PORT;
        
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
            int port = Config.DEFAULT_PORT;
            
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