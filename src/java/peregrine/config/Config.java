/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine.config;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import peregrine.*;
import peregrine.config.partitioner.*;
import peregrine.util.*;
import peregrine.io.util.*;
import peregrine.os.*;
import peregrine.sysstat.*;

import com.spinn3r.log5j.Logger;

/**
 * Config options.  See peregrine.conf for documentation of these variables.
 * 
 */
public class Config extends BaseConfig {

    private static final Logger log = Logger.getLogger();

    public Config( String host, int port ) {
        this( new Host( host, port ) );
    }

    public Config( Host host ) {
        this();
        setHost( host );
        setRoot( host );
    }

    public Config() {
        super.init( DEFAULTS );
    }

    /**
     * Init this config including partition layout and any other necesssary
     * tasks.
     */
    public void init() {

        // update the root directory from the host/port and configured basedir
        setRoot( host );

        log.info( "Using root: %s with basedir: %s", root, basedir );

        PartitionLayoutEngine engine = new PartitionLayoutEngine( this );
        engine.build();

        this.membership = engine.toMembership();

        if ( ! getHost().getName().equals( "localhost" ) &&
             ! hosts.contains( getHost() ) &&
             ! isController() ) {
            
            throw new RuntimeException( "Host is not defined in hosts file nor is it the controller: " + getHost() );
            
        }

        // the partitioner is a config directive 
        
        String partitionerDelegateClassName = getPartitionerDelegate();

        if ( ! partitionerDelegateClassName.contains( "." ) ) {
            partitionerDelegateClassName = Partitioner.class.getPackage().getName() + "." + partitionerDelegateClassName;
        }

        try {
            partitioner = (Partitioner)Class.forName( partitionerDelegateClassName ).newInstance();
            partitioner.init( this );
        } catch ( Throwable t ) {
            throw new RuntimeException( "Unable to create partitioner: ", t );
        }

        limitMaxOpenFileHandles();
        
        initEnabledFeatures();
        
        log.info( "Running with config: %s", toDesc() );

    }

    private void limitMaxOpenFileHandles() {

        // attempt to adjust the open file handles with setrlimit ... 
        if ( getMaxOpenFileHandles() > 0 ) {

            try {

                resource.RlimitStruct limit = new resource.RlimitStruct( getMaxOpenFileHandles() );

                resource.setrlimit( resource.RLIMIT.NOFILE, limit );

                log.info( "Max open file handle = %,d set via setrlimit", getMaxOpenFileHandles() );
                
            } catch ( Exception e ) {
                log.warn( "Unable to set max open file handles via setrlimit: %s ", e.getMessage() );
            }
            
        }

    }
    
    public void initEnabledFeatures() {

        // Test posix_fallocate and posix_fadvise on a test file in the
        // basedir to see if they work on this OS and if they fail disable them.

        testFallocate();
        testFadvise();

    }

    public String getRoot( Partition partition ) {
        return String.format( "%s/%s" , root , partition.getId() );
    }
        
    public String getPath( Partition partition, String path ) {
        return String.format( "%s%s" , getRoot( partition ), path );
    }

    public String getShuffleDir() {
        return String.format( "%s/tmp/shuffle", getRoot() );
    }
    
    public String getShuffleDir( String name ) {
        return String.format( "%s/%s", getShuffleDir(), name );
    }

    /**
     * Return a description of this config which is human readable.
     */
    public String toDesc() {
    	
    	try {
    		
			StringBuilder buff = new StringBuilder();
			StringBuilder multi = new StringBuilder();
			    	
			Field[] fields = BaseConfig.class.getDeclaredFields();

            Arrays.sort( fields, new Comparator<Field>() {

                    public int compare( Field f1, Field f2 ) {
                        return f1.getName().compareToIgnoreCase( f2.getName() );
                    }
                    
                } );
            
			buff.append( "\n" );
			
			for( Field field : fields ) {
                
				if ( Modifier.isStatic( field.getModifiers() ) ) {
					continue;    		
				}

                if ( field.getName().equals( "struct" ) )
                    continue;
                
				Object field_value = field.get(this);
				
				String value = null;
				
				if ( field_value != null ) 
					value = field_value.toString();
				
				if ( value != null && value.contains( "\n") ) {
					multi.append( String.format( "%s\n", value ) );
				} else {
					buff.append( String.format( "  %s = %s\n", field.getName(), value ) );
				}
		
			}
			
			buff.append( multi.toString() );
			
			return buff.toString();
			
    	} catch ( Throwable t ) {
			throw new RuntimeException( t );
		}
    
    }

    /**
     * Compute a checksum for the current config for use with determining if two
     * configs are incompatible.
     */
    public String getChecksum() {

        StringBuilder buff = new StringBuilder();

        // right now only the route and cluster membership matter.
        
        buff.append( partitioner.getClass().getName() );
        buff.append( getMembership().toString() );
        
        return Base16.encode( SHA1.encode( buff.toString() ) );
        
    }

    public long getMaxDirectMemory() {

        return getConcurrency() *
            Math.max( getShuffleBufferSize(), getSortBufferSize() );

    }

    @Override
    public String toString() {
        return String.format( "host=%s, root=%s, concurrency=%s, replicas=%s, nr_hosts=%s",
                              root, host, concurrency, replicas, hosts.size() );
    }
    
    /**
     * For a given key, in bytes, route it to the correct partition/partition.
     */
    public Partition partition( StructReader key, StructReader value ) {
    	return partitioner.partition( key, value );    
    }

    /**
     * Get a SystemProfiler for this system with the correct disks, interfaces,
     * etc used.
     */
    public SystemProfiler getSystemProfiler() {

        Set<String> interfaces = null;
        Set<String> disks      = null;
        Set<String> processors = null;

        disks = new HashSet();
        disks.add( getBasedir() );
        
        return SystemProfilerManager.getInstance( interfaces, disks, processors );
        
    }
    
    private void testFallocate() {

        testConfigOption( new RuntimeTest() {

                File file = null;

                FileOutputStream fos = null;

                @Override
                public void test() throws Exception {

                    file = new File( getBasedir(), "fallocate.test" );

                    fos = new FileOutputStream( file );

                    int fd = Platform.getFd( fos.getFD() );

                    fcntl.posix_fallocate( fd, 0, 1000 );

                }

                @Override
                public void cleanup() throws Exception {

                    new Closer( fos ).close();

                    file.delete(); //cleanup
                }

                @Override
                public void disable( Throwable t ) {
                    log.warn( "Unable to enable fallocate: %s" , t.getMessage() );
                    setFallocateExtentSize( 0 );
                }
                
            } );

    }

    private void testFadvise() {

        testConfigOption( new RuntimeTest() {

                File file = null;

                FileOutputStream fos = null;

                @Override
                public void test() throws Exception {

                    file = new File( getBasedir(), "fadvise.test" );

                    String data = "0123";
                    
                    fos = new FileOutputStream( file );
                    fos.write( data.getBytes() );

                    int fd = Platform.getFd( fos.getFD() );

                    fcntl.posix_fadvise( fd, 0, data.length(), fcntl.POSIX_FADV_DONTNEED );

                }

                @Override
                public void cleanup() throws Exception {
                    new Closer( fos ).close();
                    file.delete(); //cleanup
                }

                @Override
                public void disable( Throwable t ) {
                    log.warn( "Unable to enable fadvise: %s" , t.getMessage() );
                    setFadviseDontNeedEnabled( false );
                }
                
            } );

    }

    private void testConfigOption( RuntimeTest test ) {

        try {
            test.test();
        } catch ( Throwable t ) {
            test.disable( t );
        } finally {

            try {
                test.cleanup();
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }
                
        }
        
    }

    interface RuntimeTest {

        public void test() throws Exception;
        public void cleanup() throws Exception;
        public void disable( Throwable t );
        
    }

}
