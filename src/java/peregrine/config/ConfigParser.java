package peregrine.config;

import java.io.*;
import java.util.*;

import peregrine.util.*;

import com.spinn3r.log5j.Logger;

public class ConfigParser {

    private static final Logger log = Logger.getLogger();

    /**
     * Load the given configuration.
     */
    public static Config parse( String[] args ) throws IOException {

        String conf  = "conf/peregrine.conf";
        String hosts = "conf/peregrine.hosts";

        Config config = parse( conf, hosts );

        // we should probably convert to getopts for this.... 
        for ( String arg : args ) {

            if ( arg.startsWith( "-h=" ) ) {

                String value = arg.split( "=" )[1];
                String split[] = value.split( ":" );

                Host host = new Host( split[0], 
                                      Integer.parseInt( split[1] ) );

                log.info( "Running with custom host: %s" , host );
                
                config.setHost( host );
                
                continue;
            }

        }

        config.init();

        return config;

    }

    private static Config parse( String conf, String hosts ) throws IOException {
        return parse( new File( conf ), new File( hosts ) );
    }

    /**
     * Parse a config file from disk.
     */
    private static Config parse( File conf_file, File hosts_file ) throws IOException {

        Properties props = new Properties();
        props.load( new FileInputStream( conf_file ) );

        StructMap struct = new StructMap( props );
        
        String basedir     = struct.get( "basedir" );
        int port           = struct.getInt( "port" );

        String hostname = System.getenv( "HOSTNAME" );

        if ( hostname == null )
            hostname = "localhost";

        if ( port <= 0 )
            port = Config.DEFAULT_PORT;
        
        Config config = new Config();

        config.setBasedir( basedir );
        config.setHost( new Host( hostname, port ) );
        config.setController( Host.parse( struct.get( "controller" ) ) );

        config.setReplicas( struct.getInt( "replicas" ) );
        config.setConcurrency( struct.getInt( "concurrency" ) );

        config.setShuffleBufferSize( struct.getLong( "shuffle_buffer_size" ) );
        config.setMergeFactor( struct.getInt( "merge_factor" ) );
        
        // now read the hosts file...
        config.setHosts( readHosts( hosts_file ) );

        return config;
        
    }

    private static Set<Host> readHosts( File file ) throws IOException {

        FileInputStream fis = new FileInputStream( file );

        byte[] data = new byte[ (int)file.length() ];
        fis.read( data );

        String[] lines = new String( data ).split( "\n" );

        Set<Host> hosts = new HashSet();

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