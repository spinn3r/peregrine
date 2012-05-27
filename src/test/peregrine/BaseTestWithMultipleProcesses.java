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

    public static boolean KILL_WORKERS_ON_TEARDOWN = true;
    
    public static String MAX_MEMORY = "128M";

    /**
     * Map to store port to base directory maps.  This way unit tests can use
     * different drives and different devices.
     */
    protected static Map<Integer,String> BASEDIR_MAP = new HashMap();

    protected int concurrency = 0;
    protected int replicas = 0;
    protected int hosts = 0;

    protected Map<Integer,Process> processes = new HashMap();

    protected Map<Host,Config> configsByHost = new HashMap();

    protected List<Config> configs = new ArrayList();

    public void setUp() {

        System.out.printf( "setUp()\n" );
        
        super.setUp();

        String conf = System.getProperty( "peregrine.test.config", "1:1:1" );

        if ( conf == null || conf.equals( "" ) ) {
            log.warn( "NOT RUNNING %s: peregrine.test.config not defined", getClass().getName() );
            return;
        }

        conf = conf.trim();

        Split split = new Split( conf, ":" );
        
        concurrency = split.readInt();
        replicas    = split.readInt();
        hosts       = split.readInt();

        if ( concurrency == 0 || replicas == 0 ) {
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
            String basedir = getBasedir( port );

            Host host = new Host( "localhost", port );

            Config config = null;
            
            try {
                config = getConfig( port );
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
            
            configsByHost.put( host, config );
            configs.add( config );

            // use the files in the current basedir to see if an existing daemon
            // is running on this port and if so shut it down.

            stopDaemon( port );
            
            //clean up the previous basedir

            System.out.printf( "Removing files in %s\n", basedir );
            Files.purge( basedir );

            List<String> workerd_args = getArguments( port );

            workerd_args.add( 0, "bin/workerd" );
            workerd_args.add( "start" );

            String cmdline = Strings.join( workerd_args, " " );

            ProcessBuilder pb = new ProcessBuilder( workerd_args );

            pb.environment().put( "MAX_MEMORY",        MAX_MEMORY );
            pb.environment().put( "MAX_DIRECT_MEMORY", MAX_MEMORY );

            try {

                Pidfile pidfile = new Pidfile( config );
                
                //First make sure it's not already running by verifying that the
                //pid does not exist.
                try {

                    WaitForDaemon.waitForDaemon( pidfile.read(), port );

                    //TODO first shut it down now.
                    throw new Exception( "Daemon is already running: " + port );
                    
                } catch ( Exception e ) {
                    
                    // this is acceptable here because we want to first make
                    // sure this daemon is not running.

                    pidfile.delete();
                    
                }

                System.out.printf( "Starting proc: %s\n", cmdline );
                
                Process proc = pb.start();

                // wait for the pid file to be created OR the process exits.

                int pid = waitForProcStartup( config, proc, port );
                
                // wait for startup so we know the port is open
                WaitForDaemon.waitForDaemon( pid, port );
                
                processes.put( port, proc );

            } catch ( Throwable t ) {
                throw new RuntimeException( t );
            }
            
        }

        System.out.printf( "setUp complete and all daemons running.\n" );
        
    }

    /**
     * Wait for the proc to startup and for the pid file to be written.
     */
    private static int waitForProcStartup( Config config,
                                           Process proc,
                                           int port ) throws Exception {

        long started = System.currentTimeMillis();
        
        while( true ) {

            try {

                int exit_value = proc.exitValue();

                throw new Exception( String.format( "Proc terminated abnormally on port %s: %s" , port, exit_value ) ); 

            } catch ( IllegalThreadStateException e ) {
                //this is ok becuase the daemon hasn't terminted yet.
            }

            int pid = new Pidfile( config ).read();
            
            if ( pid > -1 ) {
                return pid;
            }

            if ( System.currentTimeMillis() - started > 30000 ) {
                throw new RuntimeException( "timeout while starting proc" );
            }
            
            Thread.sleep( 1000L );
            
        }

    }

    public List<String> getArguments( int port ) {

        List<String> list = new ArrayList();

        String basedir = getBasedir( port );

        list.add( "--hostsFile=/tmp/peregrine.hosts" );
        list.add( "--host=localhost:" + port );
        list.add( "--concurrency=" + concurrency );
        list.add( "--replicas=" + replicas );
        list.add( "--basedir=" + basedir );
        
        return list;

    }

    public String getBasedir( int port ) {

        String basedir = BASEDIR_MAP.get( port );

        if ( basedir == null )
            basedir = "/tmp/peregrine-fs-" + port;

        return basedir;
        
    }
    
    public Config getConfig() throws IOException {
        return getConfig( 11111 ); // controller port.
    }
    
    public Config getConfig( int port ) throws IOException {

        return ConfigParser.parse( Strings.toArray( getArguments( port ) ) );
        
    }

    public void tearDown() {

        System.out.printf( "tearDown()\n" );

        // for each proc, get the config, read the pid, then send kill, then
        // kill -9 if it won't shut down.

        if ( KILL_WORKERS_ON_TEARDOWN ) {
        
            for( int port : processes.keySet() ) {

                try {

                    System.out.printf( "Destroying proc on port: %s\n", port );
        
                    stopDaemon( port );

                } catch ( Exception e ) {
                    throw new RuntimeException( e );
                }
                    
            }

        }
            
        super.tearDown();
        
    }

    private void stopDaemon( int port ) {

        try {
        
            Config config = getConfig( port );
            
            peregrine.worker.Main.stop( config );

        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }

    }
    
    /**
     * Get the amount of work relative to the base test that we should be
     * working with.
     */
    public int getFactor() {

        int result = Integer.parseInt( System.getProperty( "peregrine.test.factor", "1" ) );

        log.info( "Using test factor %s", result );
        
        return result;
        
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
     * The actual test we want to run.  Implement this method and put your test
     * logic here.
     */
    public abstract void doTest() throws Exception;

    /**
     * Read all key/value pairs on the given path on all partitions.  
     */
    public List<StructPair> read( String path ) throws IOException {

        Config config = getConfig();
        
        List<StructPair> result = new ArrayList();
        
        Membership membership = config.getMembership();
        
        for( Partition part : membership.getPartitions() ) {

            Host host = membership.getHosts( part ).get( 0 );

            LocalPartitionReader reader = new LocalPartitionReader( configsByHost.get( host ), part, path );

            while( reader.hasNext() ) {

                reader.next();

                StructPair pair = new StructPair();
                pair.key = reader.key();
                pair.value = reader.value();
                
                result.add( pair );

            }

        }

        return result;
        
    }

    /**
     * 
     * Read a file on the given path and dump it to stdout.
     * 
     * b = byte
     * s = string
     * v = varint
     * i = int
     * l = long
     * f = float
     * d = double
     * B = boolean
     * c = char
     * S = short
     * h = hashcode
     * 
     */
    public void dump( String path, String key_format, String value_format ) throws IOException {

        System.out.printf( "=====\n" );
        System.out.printf( "dump:%s\n", path );
        System.out.printf( "=====\n" );
        
        List<StructPair> data = read( path );

        for( StructPair pair : data ) {
            dump( pair.key,   key_format );
            System.out.printf( "= " );
            dump( pair.value, value_format );
            
            System.out.printf( "\n" );
            
        }

    }

    private void dump( StructReader value , String format ) {

        for ( char c : format.toCharArray() ) {

            switch( c ) {
                
            case 'b':
                System.out.printf( "%s ", value.readByte() );
                break;
            case 's':
                System.out.printf( "%s ", value.readString() );
                break;
            case 'v':
                System.out.printf( "%s ", value.readVarint() );
                break;
            case 'i':
                System.out.printf( "%s ", value.readInt() );
                break;
            case 'l':
                System.out.printf( "%s ", value.readLong() );
                break;
            case 'f':
                System.out.printf( "%s ", value.readFloat() );
                break;
            case 'd':
                System.out.printf( "%s ", value.readDouble() );
                break;
            case 'B':
                System.out.printf( "%s ", value.readBoolean() );
                break;
            case 'c':
                System.out.printf( "%s ", value.readChar() );
                break;
            case 'S':
                System.out.printf( "%s ", value.readShort() );
                break;
            case 'h':
                System.out.printf( "%s ", Base16.encode( value.readHashcode() ) );
                break;

            };
            
        }
        
    }

    class StructPair {

        StructReader key;
        StructReader value;
        
    }

}
