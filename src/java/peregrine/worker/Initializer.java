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
import peregrine.util.netty.*;
import peregrine.io.util.*;

import com.spinn3r.log5j.Logger;

import org.apache.log4j.xml.DOMConfigurator;

/**
 * Initializer for bringing up the system. 
 */
public final class Initializer {

    private static final Logger log = Logger.getLogger();

	private Config config;

	public Initializer() {}

	public Initializer( Config config ) {
		this.config = config;
	}

    public void permissions() throws IOException {
        
        Files.setReadable( "conf/", false, true );
        Files.setReadableAndWritable( "logs/", false, true );
        
        Files.initDataDir( config.getRoot(),    config.getUser() );
        Files.initDataDir( config.getBasedir(), config.getUser() );
    }

    public void logger( File file ) throws IOException {
        DOMConfigurator.configure( file.getPath() );
    }

    public void logger( String suffix ) throws IOException {

        String conf = "conf/log4j.xml";

        if ( new File( conf ).canRead() == false ) {
            throw new IOException( "Unable to read: " + conf );
        }

        if ( new File( "logs" ).canWrite() == false ) {
            throw new IOException( "Unable to write to log directory" );
        }
            
        System.setProperty( "peregrine.log.suffix", suffix );
        logger( new File( conf ) );
        
    }

    public void pidfile() throws IOException {

        File file = new File( config.getRoot(), "worker.pid" );

        if ( file.exists() )
            unistd.unlink( file.getPath() );

        file = new File( config.getRoot(), "worker.tmp" );

        FileOutputStream fos = new FileOutputStream( file );
        fos.write( String.format( "%s", unistd.getpid() ).getBytes() );
        fos.close();

        unistd.rename( file.getPath(),
                       new File( config.getRoot(), "worker.pid" ).getPath() );
                       
    }

    public void setuid() throws Exception {

        int uid = unistd.getuid();

        // read the user name from the config and get the uid from the name of
        // the user.
        pwd.Passwd passwd = pwd.getpwnam( config.getUser() );

        if ( passwd == null )
            throw new Exception( "User unknown: " + config.getUser() );

        if ( passwd.uid == uid ) {
            log.info( "Already running as uid %s for user %s.", uid, config.getUser() );
            return;
        }
        
        if ( uid != 0 ) {
            throw new Exception( "Unable to setuid.  Not root: " + passwd.name );
        }

        // call setuid with the uid of the user from the passwd

        unistd.setuid( passwd.uid );

        log.info( "setuid as user %s", config.getUser() );
        
    }

    /**
     * Call setrlimit on the amount of memory we are expected to use.
     */
    public void limitMemoryUsage() {

        // one page for every file that we could potentially open.
        long max = config.getSortBufferSize();

        try {

            log.info( "Limiting locked memory (via rlimit) usage to: %,d bytes", max );

            resource.RlimitStruct limit = new resource.RlimitStruct( max );

            resource.setrlimit( resource.RLIMIT.MEMLOCK, limit );

        } catch ( Exception e ) {
            log.warn( "%s", e.getMessage() );
        }

    }

    public void requireFreeDiskSpace() throws IOException {

        // we only perform this on Linux because statfs on OS X and other
        // platforms have more struct fields.
        if ( ! Platform.isLinux() )
            return;

        if ( config.getRequireFreeDiskSpaceSize() == -1 )
            return;

        vfs.StatfsStruct struct = new vfs.StatfsStruct();
        vfs.statfs( config.getBasedir(), struct );

        long free_disk_space = struct.f_bsize * struct.f_bfree;

        if ( free_disk_space < config.getRequireFreeDiskSpaceSize() ) {
            throw new IOException( String.format( "Disk space too low: %s", free_disk_space ) );
        }
        
    }
    
    public void assertRoot() throws Exception {

        int uid = unistd.getuid();

        if ( uid != 0 ) {
            pwd.Passwd passwd = pwd.getpwuid( uid );
            throw new Exception( "Daemon must be started as root.  Currently running as " + passwd.name );
        }

    }
    
    /**
     * Perform all init steps required for the worker daemon.
     */
    public void workerd() throws Exception {

        assertRoot();
        permissions();
        requireFreeDiskSpace();
        limitMemoryUsage();
        setuid();
        basic( "workerd" );
        pidfile();
 
    }

    /**
     * Perform all init steps required for the controller.
     */
    public void controllerd() throws Exception {
        assertRoot();
        limitMemoryUsage();
        setuid();
        basic( "controllerd" );
    }

    /**
     * Perform basic init for all daemons.
     */
    public void basic( String name ) throws Exception {
        logger( String.format( "%s-%s", name, config.getHost() ) );
    }
    
    /**
     * Perform all init steps required for the controller.
     */
    public void controller() throws Exception {
        basic( "controller" );
    }
        
}