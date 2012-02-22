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

import com.spinn3r.log5j.Logger;

public class WaitForDaemon {
	
    private static final Logger log = Logger.getLogger();
        
    private static Config config = null;
        
    public static void main(String[] args ) throws Exception {

        int pid      = Integer.parseInt( args[0] );
        int port     = Integer.parseInt( args[1] );
        int timeout  = Integer.parseInt( args[2] );

        long now = System.currentTimeMillis();

        InetAddress localhost = InetAddress.getLocalHost();
        SocketAddress addr = new InetSocketAddress( localhost, port );
        
        while( true ) {

            Socket sock = new Socket();

            try {
                signal.kill( pid, 0 );
            } catch ( PlatformException e ) {
                System.out.printf( "ERROR: process with pid %s is dead.\n", pid );
                System.exit( 1 );
            }
            
            try {
                sock.connect( addr, 1000 );
                //success ... we are up so just exit.
                System.out.printf( "SUCCESS: daemon up and listening on port %,d\n", port );
                System.exit( 0 );
                
            } catch ( IOException e ) { }

            if ( System.currentTimeMillis() > now + timeout ) {

                System.out.printf( "ERROR: exceeded timeout: %,d ms\n", timeout );
                System.exit( 1 );
                
            }
            
        }
        
    }
   
}
    
