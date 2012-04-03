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
package peregrine;

import java.io.*;
import java.util.*;

import peregrine.config.*;
import peregrine.worker.*;
import peregrine.util.*;
import peregrine.io.util.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;

import com.spinn3r.log5j.Logger;

public abstract class BaseTestWithMultipleProcesses extends peregrine.BaseTest {

    private static final Logger log = Logger.getLogger();

    public static String MAX_MEMORY = "128M";
    
    protected int concurrency = 0;
    protected int replicas = 0;
    protected int hosts = 0;

    protected Map<String,Process> processes = new HashMap();

    protected Map<Host,Config> configsByHost = new HashMap();

    public void setUp() {

        super.setUp();

        String conf = System.getProperty( "peregrine.test.config" );

        if ( conf == null ) {
            log.warn( "NOT RUNNING %s: peregrine.test.config not defined", getClass().getName() );
            return;
        }

        conf = conf.trim();

        Split split = new Split( conf, ":" );
        
        concurrency = split.readInt();
        replicas    = split.readInt();
        hosts       = split.readInt();

        if ( concurrency == 0 ) {
            log.error( "Concurrency was zero" );
            return;
        }
        
        log.info( "Working with concurrency=%s, replicas=%s, hosts=%s" , concurrency, replicas, hosts );

        //Write out a new peregrine.hosts file.

        try {
            
            FileOutputStream fos = new FileOutputStream( "/tmp/peregrine.hosts" );

            for( int i = 0; i < hosts; ++i ) {

                int port = 11112 + i;
                fos.write( String.format( "localhost:%s\n", port ).getBytes() );
                
            }

            fos.close();

        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
            
        for( int i = 0; i < hosts; ++i ) {

            // startup new daemons no different port and in different
            // directories.

            int port = 11112 + i;
            String basedir = "/tmp/peregrine-fs-" + i;

            Host host = new Host( "localhost", port );

            //clean up the previosu basedir
            Files.remove( basedir );

            List<String> workerd_args = getArguments( port );

            workerd_args.add( 0, "bin/workerd" );
            workerd_args.add( "--basedir=" + basedir );
            workerd_args.add( "start" );

            List<String> bash_args = new ArrayList();
            
            bash_args.add( "/bin/bash" );
            bash_args.add( "-c" );
            bash_args.add( "'" + Strings.join( workerd_args , " " ) + "'" );
            bash_args.add( "> foo.log" );
            
            /*
            args.add( 0, "bin/workerd" );
            args.add( "--basedir=" + basedir );
            args.add( "start" );
            */
            
            String cmdline = Strings.join( bash_args, " " );

            ProcessBuilder pb = new ProcessBuilder( bash_args );

            pb.environment().put( "MAX_MEMORY",        MAX_MEMORY );
            pb.environment().put( "MAX_DIRECT_MEMORY", MAX_MEMORY );

            try {

                System.out.printf( "Starting proc: %s\n", cmdline );

                Process proc = pb.start();

                // wait for the pid file to be created OR the process exits.

                int pid = waitForProc( basedir,
                                       proc,
                                       port );

                WaitForDaemon.waitForDaemon( pid, port );
                
                processes.put( cmdline, proc );

                // wait for startup...
                
            } catch ( Throwable t ) {
                throw new RuntimeException( t );
            }
            
        }

        System.out.printf( "setUp complete and all daemons running.\n" );
        
    }

    private static int waitForProc( String basedir,
                                    Process proc,
                                    int port ) throws Exception {

        long started = System.currentTimeMillis();

        ///tmp/peregrine-fs//localhost/11112/worker.pid
        
        File pidfile = new File( String.format( "%s/localhost/%s/worker.pid",
                                                basedir,
                                                port ) );
        
        while( true ) {

            try {

                proc.exitValue();

                throw new Exception( "Proc terminated abnormally: " + port );

            } catch ( IllegalThreadStateException e ) {
                //this is ok becuase the daemon hasn't terminted yet.
            }

            if ( pidfile.exists() ) {

                return peregrine.worker.Main.readPidfile( pidfile );
                
            }

            if ( System.currentTimeMillis() - started > 30000 ) {
                throw new RuntimeException( "timeout while starting proc" );
            }
            
            Thread.sleep( 1000L );
            
        }

    }

    public List<String> getArguments( int port ) {

        List<String> list = new ArrayList();
        
        list.add( "--hostsFile=/tmp/peregrine.hosts" );
        list.add( "--host=localhost:" + port );
        list.add( "--concurrency=" + concurrency );
        list.add( "--replicas=" + replicas );

        return list;

    }

    public Config getConfig() throws IOException {
        return getConfig( 11111 ); // controller port.
    }
    
    public Config getConfig( int port ) throws IOException {

        return ConfigParser.parse( Strings.toArray( getArguments( port ) ) );
        
    }

    public void tearDown() {

        for( String cmdline : processes.keySet() ) {
            Process proc = processes.get( cmdline );
            System.out.printf( "Destroying proc: %s\n", cmdline );
            proc.destroy();
        }
        
        super.tearDown();
        
    }

    /**
     * Get the amount of work relative to the base test that we should be
     * working with.
     */
    public int getFactor() {

        String factor = System.getProperty( "peregrine.test.factor" );

        if ( factor == null )
            return 1;

        return Integer.parseInt( factor );
        
    }
    
    public void test() throws Exception {

        try {

            doTest();

        } catch ( Throwable t ) {

            if ( t instanceof Exception )
                throw (Exception)t;

            if ( t instanceof RuntimeException )
                throw (RuntimeException)t;

            throw new RuntimeException( t );
            
        } finally {

        }

    }

    /**
     * The actual test we want to run....
     */
    public abstract void doTest() throws Exception;

}
