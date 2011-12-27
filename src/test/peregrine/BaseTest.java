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

import peregrine.io.util.*;
import peregrine.config.*;

import org.junit.runner.*;

public abstract class BaseTest extends junit.framework.TestCase {

    public void setUp() {

        remove( Config.DEFAULTS.getString( "basedir" ) );

        //org.apache.log4j.MDC.put( "server.hostname",    Initializer.HOSTNAME );
        
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
    
    /**
     * Method to allow ALL junit classes to be called from the command line
     * which allows for us having less main() methods cluttering up the test
     * suite.
     */
    public static void runTests() throws Exception {

        String classname = Thread.currentThread().getStackTrace()[2].getClassName();

        JUnitCore core = new JUnitCore();
        
        core.run( Class.forName( classname ) );

    }

}
