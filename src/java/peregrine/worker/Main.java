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
import java.util.*;

import peregrine.config.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 * Command line main() class for the worker daemon.
 */
public class Main {
	
    private static final Logger log = Logger.getLogger();

    public static void stop( String[] args ) throws Exception {

       Config config = ConfigParser.parse( args );

       stop( config );
       
    }
    
    public static void stop( Config config ) throws Exception {

        new Initializer( config ).assertRoot();

        log.info( "Stopping on %s" , config.getHost() );
        
        // read the pid file...
        int pid = new Pidfile( config ).read();

        if ( pid == -1 ) {
            return;
        }
        
        if ( WaitForDaemon.running( pid ) == false ) {
            return;
        }

        // send the kill
        signal.kill( pid, signal.SIGTERM );
        
        // see if the daemon is still running
        long now = System.currentTimeMillis();

        while( WaitForDaemon.running( pid ) ) {

            Thread.sleep( 1000L );
            System.out.printf( "." );
            
        }

        // after we are done waiting, kill -9 it.
        if( WaitForDaemon.running( pid ) ) {
            // force it to terminate
            System.out.printf( "X" );
            signal.kill( pid, signal.SIGKILL );
        }

        System.out.printf( "\n" );
        
    }

    private static void start( String[] args ) throws Exception {

        Config config = ConfigParser.parse( args );

        log.info( "Starting on %s with controller: %s" , config.getHost(), config.getController() );

        new Initializer( config ).workerd();

        log.info( "Running with config: \n%s", config.toDesc() );

        new FSDaemon( config );

        System.out.printf( "Daemon up and running on %s\n", config.getHost() );
        
        Thread.sleep( Long.MAX_VALUE );

    }

    public static void main( String[] args ) throws Exception {

        if ( args.length == 0 ) {
            System.out.printf( "SYNTAX start|stop\n" );
            System.exit( 1 );
        }

        String command = args[ args.length - 1 ];
        
        if ( "start".equals( command ) ) {
            start( args );
        } 

        if ( "stop".equals( command ) ) {
            stop( args );
        }

    }
        
}
    
