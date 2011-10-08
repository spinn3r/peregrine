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
import peregrine.shuffle.*;
import peregrine.io.*;
import peregrine.io.async.*;
import peregrine.perf.*;

public abstract class BaseTest extends junit.framework.TestCase {

    public void setUp() {

        // init log4j ... 
        org.apache.log4j.xml.DOMConfigurator.configure( "conf/log4j.xml" );

        System.out.printf( "Cleaning up PFS_ROOT: %s\n", Config.PFS_ROOT );
        DiskPerf.remove( Config.PFS_ROOT );

    }

    public void tearDown() {
        System.out.printf( "Cleaning up PFS_ROOT: %s\n", Config.PFS_ROOT );
        DiskPerf.remove( Config.PFS_ROOT );

        AsyncOutputStreamService.shutdown();
        com.spinn3r.log5j.LogManager.shutdown();

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

}
