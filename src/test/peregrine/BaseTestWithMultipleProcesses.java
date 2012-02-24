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
import peregrine.io.chunk.*;
import peregrine.io.partition.*;

import com.spinn3r.log5j.Logger;

public abstract class BaseTestWithMultipleProcesses extends peregrine.BaseTest {

    private static final Logger log = Logger.getLogger();

    protected int concurrency = 0;
    protected int replicas = 0;
    protected int hosts = 0;

    protected Map<String,Process> processes = new HashMap();
    
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

        for( int i = 0; i < hosts; ++i ) {

            // startup new daemons no different port and in different
            // directories.

            int port = 11112 + i;
            String basedir = "/tmp/peregrine-fs-" + i;

            List<String> args = Arrays.asList( "bin/workerd",
                                               "--host=localhost:" + port ,
                                               "--basedir=" + basedir );

            String cmdline = Strings.join( args, " " );
            
            System.out.printf( "starting proc: %s\n", cmdline );
            
            ProcessBuilder pb = new ProcessBuilder( args );

            pb.environment().put( "MAX_MEMORY", "32M" );
            pb.environment().put( "MAX_DIRECT_MEMORY", "32M" );

            try {
                
                Process proc = pb.start();
                
                processes.put( cmdline, proc );

            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
            
        }

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
