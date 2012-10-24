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
import peregrine.os.*;
import peregrine.os.proc.*;
import peregrine.rpc.*;

import com.spinn3r.log5j.Logger;

/**
 * Base test for running with multiple daemons forked under the test harness.
 */
public abstract class BaseTestWithMultipleProcesses extends peregrine.BaseTest {

    private static final Logger log = Logger.getLogger();

    public static long TIMEOUT = 2 * 60 * 1000;
    
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

    /**
     * Specify extra arguments to worker daemons.
     */
    protected List<String> extraWorkerArguments = new ArrayList();
    
    private boolean readConfig() {

        String conf = System.getProperty( "peregrine.test.config", "1:1:1" );

        if ( conf == null || conf.equals( "" ) ) {
            log.warn( "NOT RUNNING %s: peregrine.test.config not defined", getClass().getName() );
            return false;
        }

        conf = conf.trim();

        Split split = new Split( conf, ":" );
        
        concurrency = split.readInt();
        replicas    = split.readInt();
        hosts       = split.readInt();

        if ( concurrency == 0 || replicas == 0 ) {
            log.error( "Concurrency was zero" );
            return false;
        }

        return true;
        
    }

    @Override
    public void setUp() {

        System.out.printf( "setUp()\n" );

        super.setUp();

        try {
            killAllDaemons();
        } catch ( Exception e ) {
            RuntimeException rte = new RuntimeException( "Unable to kill daemons: " );
            rte.initCause( e );
            throw rte;
        }
        
        if ( readConfig() == false ) {
            return;
        }
        
        log.info( "Working with concurrency=%s, replicas=%s, hosts=%s" , concurrency, replicas, hosts );

        //Write out a new peregrine.hosts file.

        try {
            
            FileOutputStream fos = new FileOutputStream( "/tmp/peregrine.hosts" );

            for( int i = 0; i < hosts; ++i ) {

                int port = Host.DEFAULT_PORT + i;
                fos.write( String.format( "localhost:%s\n", port ).getBytes() );
                
            }

            fos.close();

        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
            
        for( int i = 0; i < hosts; ++i ) {

            // startup new daemons no different port and in different
            // directories.

            int port = Host.DEFAULT_PORT + i;
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

            //clean up the previous basedir

            System.out.printf( "Removing files in %s\n", basedir );

            try {
                Files.purge( basedir );
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }

            List<String> workerd_args = getArguments( port );

            workerd_args.add( 0, "bin/workerd" );
            workerd_args.add( "start" );

            String cmdline = Strings.join( workerd_args, " " );

            ProcessBuilder pb = new ProcessBuilder( workerd_args );

            pb.environment().put( "MAX_MEMORY",        MAX_MEMORY );
            pb.environment().put( "MAX_DIRECT_MEMORY", MAX_MEMORY );
            pb.environment().put( "OUTPUT",            "standard" );

            try {

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

    private void killAllDaemons() throws Exception {

        ProcessList ps = new ProcessList();

        for( ProcessListEntry proc : ps.getProcesses() ) {

            List<String> arguments = proc.getArguments();

            if ( arguments.size() <= 1 )
                continue;

            if ( ! "java".equals( arguments.get( 0 ) ) ) {
                continue;
            }

            boolean isDaemon = false;

            for( String arg : arguments ) {
                if ( peregrine.worker.Main.class.getName().equals( arg ) ) {
                    isDaemon = true;
                    break;
                }
            }

            if ( isDaemon ) {
                
                int pid = proc.getId();
                
                System.out.printf( "Sending SIGTERM to %s\n", proc  );
                signal.kill( pid, signal.SIGTERM );
                
            }
            
        }

    }

    /**
     * Wait for the proc to startup and for the pid file to be written.
     */
    private static int waitForProcStartup( Config config,
                                           Process proc,
                                           int port ) throws Exception {

        System.out.printf( "Waiting for startup on port %s ", port );
        
        long started = System.currentTimeMillis();
        
        while( true ) {

            int pid = new Pidfile( config ).read();
            
            if ( pid > -1 ) {
                System.out.printf( "done\n" );
                return pid;
            }

            if ( System.currentTimeMillis() - started > TIMEOUT ) {
                throw new RuntimeException( "timeout while starting proc." );
            }

            System.out.printf( "." );
            
            Thread.sleep( 1000L );
            
        }

    }

    public List<String> getArguments( int port ) {

        List<String> list = new ArrayList( extraWorkerArguments );

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
            basedir = "/tmp/peregrine/fs-" + port;

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

        super.tearDown();
        
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

            try {
                killAllDaemons();
            } catch ( Exception e ) {
                log.error( "Unable to kill daemons: ", e );
            }

            try {
                readWorkerOutput();
            } catch ( Exception e ) {
                log.error( "Unable to read worker output: ", e );
            }

        }

    }

    private void readWorkerOutput() throws IOException {

        for( int i = 0; i < hosts; ++i ) {

            // startup new daemons no different port and in different
            // directories.

            int port = Host.DEFAULT_PORT + i;

            //include( new File( String.format( "logs/peregrine-workerd-localhost:%s.log", port ) ) );
            //include( new File( String.format( "logs/peregrine-workerd-localhost:%s.err", port ) ) );
            
        }
            
    }

    private void include( File file ) throws IOException {

        if ( file.exists() ) {
            System.out.printf( "================================= %s\n", file.getPath() );
        } else {
            return;
        }

        byte[] data = new byte[4096];

        FileInputStream fis = null;

        try {

            fis = new FileInputStream( file );

            while( true ) {

                int read = fis.read( data );

                System.out.write( data, 0, read );
                
                if ( read != data.length )
                    break;
                
            }
            
        } finally {
            new Closer( fis ).close();
        }

    }
    
    /**
     * The actual test we want to run.  Implement this method and put your test
     * logic here.
     */
    public abstract void doTest() throws Exception;

    /**
     * Read all key/value pairs on the given path on all partitions... 
     */
    public List<StructPair> read( String path ) throws IOException {

        Config config = getConfig();
        
        List<StructPair> result = new ArrayList();
        
        Membership membership = config.getMembership();

        // we have to go through the earlier partitions first followed by later
        // partitions because when we are testing sorted output it is important
        // to get the right order.
        
        List<Partition> partitions = new ArrayList( membership.getPartitions() );
        Collections.sort( partitions );
        
        for( Partition part : partitions ) {

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

}
