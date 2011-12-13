package peregrine.config;

import java.io.*;
import java.util.*;

import peregrine.io.util.*;
import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 * Handles parsing the peregrine.conf file and the peregrine.hosts file as well
 * as handling command line arguments.
 */
public class ConfigParser {

    protected static final Logger log = Logger.getLogger();

    public static Config parse() throws IOException {
        return parse( new String[0] );
    }

    /**
     * Load the given configuration.
     */
    public static Config parse( String[] args ) throws IOException {

        String conf  = "conf/peregrine.conf";
        String hosts = "conf/peregrine.hosts";

        Config config = parse( conf, hosts );

        Getopt getopt = new Getopt( args );

        for ( String key : config.struct.getKeys() ) {

            if( getopt.containsKey( key ) ) {
                config.struct.put( key, getopt.getString( key ) );
            }
            
        }

        // re-init the config with the params from the command line.  With no
        // param specified this is essentially idempotent.
        config.init( config.struct );
        
        config.init();

        return config;

    }

    protected static Config parse( String conf, String hosts ) throws IOException {
        return parse( new File( conf ), new File( hosts ) );
    }

    /**
     * Parse a config file from disk.
     */
    protected static Config parse( File conf_file, File hosts_file ) throws IOException {

        Config config = parse( new FileInputStream( conf_file ) );
        
        String hostname = determineHostname();

        config.setHost( new Host( hostname, config.getPort() ) );

        // now read the hosts file...
        config.setHosts( readHosts( hosts_file ) );

        return config;
        
    }

    protected static Config parse( InputStream is ) throws IOException {

        StructMap struct = new StructMap( is );

        Config config = new Config();

        config.init( struct );

        return config;
        
    }
    
    protected static Set<Host> readHosts( File file ) throws IOException {

        String data = Files.toString( file );
        
        String[] lines = data.split( "\n" );

        Set<Host> hosts = new HashSet();

        for( String line : lines ) {

            line = line.trim();
            
            if ( "".equals( line ) )
                continue;

            if ( line.startsWith( "#" ) )
                continue;

            Host host = Host.parse( line );
            hosts.add( host);
            
        }

        return hosts;
        
    }

    /**
     * Try to determine the hostname from the current machine.  This includes
     * reading /etc/hostname, looking at the HOSTNAME environment variable, etc.
     */
    protected static String determineHostname() throws IOException {

        String hostname = System.getenv( "HOSTNAME" );

        File file = new File( "/etc/hostname" );

        if ( file.exists() ) {

            hostname = Files.toString( file );
 
        }
        
        if ( hostname == null )
            hostname = "localhost";

        hostname = hostname.trim();
        hostname = hostname.replaceAll( "\n" , "" );
        hostname = hostname.replaceAll( "\r" , "" );
        hostname = hostname.replaceAll( "\t" , "" );
        
        return hostname;
        
    }

}