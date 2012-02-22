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
package peregrine.worker;

import java.io.*;
import java.net.*;

import peregrine.config.*;
import peregrine.os.*;
import peregrine.util.*;

import com.spinn3r.log5j.Logger;

/**
 * With a given pid, port, and timeout, wait until either the port is
 * successfully opened and ready for IO or the pid terminates.
 * 
 * Returns 1 when the pid is nonexistant or the port isn't opened within the
 * timeout window.
 *
 * Returns 0 once the port is open.
 *
 * When no pid is specified, we just poll if the port is open and return 0 if it
 * is open and 1 if it is closed.
 * 
 */
public class WaitForDaemon {
	
    private static final Logger log = Logger.getLogger();

    public static final long TIMEOUT = 30000;

    public static void main(String[] args ) throws Exception {

        Config config = ConfigParser.parse( args );
        Getopt getopt = new Getopt( args );
        
        int pid      = getopt.getInt( "pid", -1 );
        int port     = config.getHost().getPort();

        long now = System.currentTimeMillis();

        InetAddress localhost = InetAddress.getLocalHost();
        SocketAddress addr = new InetSocketAddress( localhost, port );
        
        while( true ) {

            Socket sock = new Socket();

            if ( pid > -1 ) {
                
                try {
                    signal.kill( pid, 0 );
                } catch ( PlatformException e ) {
                    System.out.printf( "ERROR: process with pid %s is dead.\n", pid );
                    System.exit( 1 );
                }

            }
                
            try {

                System.out.printf( "." );
                
                sock.connect( addr, 1000 );
                //success ... we are up so just exit.

                System.out.printf( "\n" );
                
                System.out.printf( "SUCCESS: daemon up and listening on port %s\n", port );
                System.exit( 0 );
                
            } catch ( IOException e ) { }

            if ( pid == -1 ) {
                System.exit( 1 );
            }

            Thread.sleep( 1000L );

            if ( System.currentTimeMillis() > now + TIMEOUT ) {

                System.out.printf( "ERROR: exceeded timeout: %,d ms\n", TIMEOUT );
                System.exit( 1 );
                
            }
            
        }
        
    }
   
}
    
