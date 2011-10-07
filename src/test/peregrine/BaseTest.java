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
import peregrine.perf.*;

public abstract class BaseTest extends junit.framework.TestCase {

    public void setUp() {
        System.out.printf( "Cleaning up PFS_ROOT: %s\n", Config.PFS_ROOT );
        DiskPerf.remove( Config.PFS_ROOT );
    }

    public void tearDown() {
        System.out.printf( "Cleaning up PFS_ROOT: %s\n", Config.PFS_ROOT );
        DiskPerf.remove( Config.PFS_ROOT );
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
