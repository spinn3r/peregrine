package peregrine;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import java.security.*;

import peregrine.*;
import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.reduce.*;
import peregrine.config.Config;
import peregrine.io.*;
import peregrine.io.async.*;

public abstract class BaseTest extends junit.framework.TestCase {

    public void setUp() {

        remove( Config.DEFAULT_ROOT );
        
        // init log4j ... 
        org.apache.log4j.xml.DOMConfigurator.configure( "conf/log4j.xml" );

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

    public static void remove( String path ) {
        remove( new File( path ) );
    }

    public static void remove( File file ) {

        if ( ! file.exists() )
            return;

        File[] files = file.listFiles();
        
        for ( File current : files ) {

            if ( current.isDirectory() == false ) {
                System.out.printf( "Deleting: %s\n", current.getPath() );
                current.delete();
            } else {
                remove( current );
            }
            
        }
        
    }

    /**
     * Method to allow ALL junit classes to be called from the command line
     * which allows for us having less main() methods cluttering up the test
     * suite.
     */
    public static void runTests() throws Exception {

        String classname = Thread.currentThread().getStackTrace()[2].getClassName();
        org.junit.runner.JUnitCore.main( classname );

    }

}
