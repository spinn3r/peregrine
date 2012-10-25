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

    /**
     * Return true if the given pid is running.
     */
    public static boolean running( int pid ) {

        try {

            signal.kill( pid, 0 );
            return true;

        } catch ( PlatformException e ) {
            return false;
        }

    }

    public static void waitForDaemon( int pid, int port ) throws Exception {

        long now = System.currentTimeMillis();

        while( true ) {

            InetAddress localhost = InetAddress.getLocalHost();
            SocketAddress addr = new InetSocketAddress( localhost, port );

            Socket sock = new Socket();

            if ( pid > -1 ) {

                if ( running( pid ) == false ) {
                    throw new Exception( String.format( "Process with pid %s on port %s is dead.", pid, port ) );
                }

            }
                
            try {

                sock.connect( addr, 5000 );
                //success ... we are up so just exit.

                System.out.printf( "\n" );
                
                System.out.printf( "SUCCESS: daemon up and listening on port %s\n", port );
                return;
                
            } catch ( IOException e ) {
                System.out.printf( "FIXME: unable to connect to \n" );
                e.printStackTrace();
            }

            if ( pid == -1 ) {
                throw new Exception( "Process pid not running" );
            }

            System.out.printf( "." );

            Thread.sleep( 1000L );

            if ( System.currentTimeMillis() > now + TIMEOUT ) {

                String msg = String.format( "Exceeded timeout %,d ms waiting for pid %s on port %s at sock address %s",
                                            TIMEOUT, pid, port, addr );

                System.err.printf( "FIXME: %s\n", msg );
                
                System.err.printf( "FIXME: sleeping forever to help you debug \n" );
                
                Thread.sleep( Long.MAX_MEMORY );
                
                throw new Exception( msg );
                
            }
            
        }

    }
    
    public static void main( String[] args ) throws Exception {

        Config config = ConfigParser.parse( args );
        Getopt getopt = new Getopt( args );
        
        int pid      = getopt.getInt( "pid", -1 );
        int port     = config.getHost().getPort();

        try {
            waitForDaemon( pid, port );
        } catch ( Exception e ) {
            System.out.printf( "Failed to startup daemon: %s\n", e.getMessage() );
            e.printStackTrace();
            System.exit( 1 );
        }

    }
   
}
    
