package peregrine.config;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import peregrine.config.router.*;
import peregrine.util.primitive.*;
import peregrine.util.*;
import peregrine.os.*;

import com.spinn3r.log5j.Logger;

/**
 * Config options.  See peregrine.conf for documentation of these variables.
 * 
 */
public class Config extends BaseConfig {

    private static final Logger log = Logger.getLogger();

    public Config() { }

    public Config( String host, int port ) {
        this( new Host( host, port ) );
    }

    public Config( Host host ) {
        setHost( host );
        setRoot( host );
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

        if ( ! hosts.contains( getHost() ) && ! isController() ) {
            throw new RuntimeException( "Host is not defined in hosts file nor is it the controller: " + getHost() );
        }

        // TODO: move the hash partitioner implementation to a config directive... 
        
        router = new HashPartitionRouter();
        router.init( this );

        new File( root ).mkdirs();

        initEnabledFeatures();
        
        log.info( "%s", toDesc() );

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
			    	
			Field[] fields = getClass().getDeclaredFields();
			
			buff.append( "\n" );
			
			for( Field field : fields ) {
				
				if ( Modifier.isStatic( field.getModifiers() ) ) {
					continue;    		
				}
				
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

    public String getChecksum() {

        StringBuilder buff = new StringBuilder();

        // right now only the route and cluster membership matter.
        
        buff.append( router.getClass().getName() );
        buff.append( getMembership().toString() );
        
        return Base16.encode( SHA1.encode( buff.toString() ) );
        
    }

    @Override
    public String toString() {
        return String.format( "host=%s, root=%s, concurrency=%s, replicas=%s, nr_hosts=%s",
                              root, host, concurrency, replicas, hosts.size() );
    }
    
    /**
     * For a given key, in bytes, route it to the correct partition/partition.
     */
    public Partition route( byte[] key ) {
    	return router.route( key );    
    }

    private void testFallocate() {

        testConfigOption( new RuntimeTest() {

                File file = null;

                FileOutputStream fos = null;

                @Override
                public void test() throws Exception {

                    file = new File( getBasedir(), "fallocate.test" );

                    fos = new FileOutputStream( file );

                    int fd = Native.getFd( fos.getFD() );

                    fcntl.posix_fallocate( fd, 0, 1000 );

                }

                @Override
                public void cleanup() throws Exception {
                    fos.close();
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

                    int fd = Native.getFd( fos.getFD() );

                    fcntl.posix_fadvise( fd, 0, data.length(), fcntl.POSIX_FADV_DONTNEED );

                }

                @Override
                public void cleanup() throws Exception {
                    fos.close();
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