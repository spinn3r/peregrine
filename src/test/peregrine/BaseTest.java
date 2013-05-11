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

import peregrine.io.util.*;
import peregrine.io.partition.*;
import peregrine.config.*;
import peregrine.os.*;

import org.junit.runner.*;
import org.junit.runner.notification.*;

public abstract class BaseTest extends junit.framework.TestCase {

    public static boolean REMOVE_BASEDIR = true;
    
    public void setUp() {

        System.out.printf( "================================================================================\n" );
        
        if ( REMOVE_BASEDIR ) {
            try {
                Files.purge( Config.DEFAULTS.getString( "basedir" ) );
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }

        //org.apache.log4j.MDC.put( "server.hostname",    Initializer.HOSTNAME );
        
        // init log4j ...

        org.apache.log4j.xml.DOMConfigurator.configure( "conf/log4j.xml" );

        // tell the OS to sync before we begin this test.  With LOTS of tests it
        // is possible dirty the VFS page cache so we need to make sure to sync
        // before hand.
        unistd.sync();
        
    }

    public void tearDown() {

    }

    public static byte[] toByteArray( InputStream is ) throws IOException {

        //include length of content from the original site with contentLength
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
      
        //now process the Reader...
        byte data[] = new byte[2048];
    
        int readCount = 0;

        while( ( readCount = is.read( data )) > 0 ) {
            bos.write( data, 0, readCount );
        }

        is.close();
        bos.close();

        return bos.toByteArray();

    }

    public static void copy( File source, File target ) throws IOException {

        FileInputStream in = null;
        FileOutputStream out = null;

        try {

            in = new FileInputStream( source );
            out = new FileOutputStream( target );

            out.getChannel().transferFrom( in.getChannel(), 0, source.length() );

        } finally {
            new Closer( in, out ).close();
        }

    }

    public static void setPropertyDefault( String key, String value ) {

        if ( System.getProperty( key ) == null )
            System.setProperty( key , value );
        
    }

    // get a list of StructReader between the given range (inclusive)
    protected List<StructReader> range( long start, long end ) {
    
        List<StructReader> result = new ArrayList();

        for( long i = start; i <= end; ++i ) {
            result.add( StructReaders.wrap( i ) );
        }

        return result;
        
    }

    /**
     * Method to allow ALL junit classes to be called from the command line
     * which allows for us having less main() methods cluttering up the test
     * suite.
     */
    public static void runTests() throws Exception {

        String classname = Thread.currentThread().getStackTrace()[2].getClassName();

        JUnitCore core = new JUnitCore();
        
        Result result = core.run( Class.forName( classname ) );

        List<Failure> failures = result.getFailures();

        for( Failure fail : failures ) {
            fail.getException().printStackTrace();
        }
        
        if ( failures.size() > 0 )
            System.exit( 1 );

        System.out.printf( "run count: %s\n", result.getRunCount() );
        System.out.printf( "was successful: %s\n", result.wasSuccessful() );
        
    }

}
