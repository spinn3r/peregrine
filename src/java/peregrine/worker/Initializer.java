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

import org.apache.log4j.xml.DOMConfigurator;

/**
 * Initializer for bringing up the system. 
 */
public final class Initializer {

    private static final Logger log = Logger.getLogger();

	private Config config;
	
	public Initializer( Config config ) {
		this.config = config;
	}
	
    public void logger() {

        System.setProperty( "peregrine.host", "" + config.getHost() );
        
        DOMConfigurator.configure( "conf/log4j.xml" );

    }

    public void pidfile() throws IOException {

        File file = new File( config.getRoot(), "worker.pid" );
        FileOutputStream fos = new FileOutputStream( file );
        fos.write( String.format( "%s", unistd.getpid() ).getBytes() );
        fos.close();

    }

    public void setuid() throws Exception {

        if ( unistd.getuid() != 0 ) {
            log.warn( "Unable to setuid.  Not root." );
            return;
        }
        
        // read the user name from the config and get the uid from the name of
        // the user.

        pwd.Passwd passwd = pwd.getpwnam( config.getUser() );

        if ( passwd == null )
            throw new Exception( "User unknown: " + config.getUser() );

        // call setuid with the uid of the user from the passwd

        unistd.setuid( passwd.uid );

        log.info( "setuid as user %s", config.getUser() );
        
    }

    /**
     * Call setrlimit on the amount of memory we are expected to use.
     */
    public void limitMemoryUsage() {

        // one page for every file that we could potentially open.
        long max = config.getShuffleSegmentMergeParallelism() * (unistd.getpagesize() * 2);

        try {

            log.info( "Limiting locked memory usage to: %,d bytes", max );

            resource.rlimit limit = new resource.rlimit( max );

            resource.setrlimit( resource.RLIMIT.MEMLOCK, limit );

        } catch ( Exception e ) {
            log.warn( "%s", e.getMessage() );
        }

    }

    /**
     * Perform all init steps required for the worker daemon.
     */
    public void init() throws Exception {

        logger();
        pidfile();
        setuid();
        limitMemoryUsage();

    }
    
}