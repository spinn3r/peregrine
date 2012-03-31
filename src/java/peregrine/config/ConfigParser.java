/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
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

    private static final Logger log = Logger.getLogger();

    /**
     * Load the given configuration.
     */
    public static Config parse( String... args ) throws IOException {

        String conf  = "conf/peregrine.conf";

        Config config = parse( conf );

        Getopt getopt = new Getopt( args );

        for ( String key : config.struct.getKeys() ) {

            // override with options provide on the command line.
            if( getopt.containsKey( key ) ) {
                config.struct.put( key, getopt.getString( key ) );
            }
            
        }

        // the 'host' specified on the command line is the only special command
        // line option right now since we don't define this in the .conf.

        if ( getopt.containsKey( "host" ) ) {
            config.setHost( getopt.getString( "host" ) );
        }
        
        // re-init the config with the params from the command line.  With no
        // param specified this is essentially idempotent.
        config.init( config.struct );

        //TODO: we should log which hosts file we are reading from.

        log.info( "Parsing host file: %s", config.getHostsFile() );
        
        // now read the hosts file...
        config.setHosts( readHosts( new File( config.getHostsFile() ) ) );

        log.info( "Read %,d hosts from hosts file." , config.getHosts().size() );
        
        config.init();

        return config;

    }

    protected static Config parse( String conf ) throws IOException {
        return parse( new File( conf ) );
    }

    /**
     * Parse a config file from disk.
     */
    protected static Config parse( File conf_file ) throws IOException {

        Config config = parse( new FileInputStream( conf_file ) );

        String hostname = determineHostname();

        config.setHost( new Host( hostname, config.getPort() ) );

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
        
        if ( hostname == null ) {
        
            File file = new File( "/etc/hostname" );

            if ( file.exists() ) {
                hostname = Files.toString( file );
            }

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
