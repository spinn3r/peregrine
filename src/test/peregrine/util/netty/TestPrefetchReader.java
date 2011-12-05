package peregrine.util.netty;

import java.io.*;
import java.util.*;

import peregrine.keys.*;
import peregrine.values.*;
import peregrine.util.*;
import peregrine.util.primitive.LongBytes;
import peregrine.reduce.*;
import peregrine.reduce.merger.*;
import peregrine.io.chunk.*;
import peregrine.config.*;
import peregrine.os.*;

public class TestPrefetchReader extends peregrine.BaseTest {

    public void test1() throws Exception {

        // create a test file ...
        File file = new File( "/tmp/test.dat" );
        FileOutputStream out = new FileOutputStream( file );

        byte[] data = new byte[ (int)PrefetchReader.DEFAULT_PAGE_SIZE ]; 
        
        for( int i = 0; i < 20; ++i ) {
            out.write( data );
        }

        out.close();

        Config config = new Config();
        
        MappedFile mappedFile = new MappedFile( config, file, "r" );
        
        List<MappedFile> files = new ArrayList();
        files.add( mappedFile );

        StreamReader reader = new StreamReader( mappedFile.map() );

        PrefetchReader prefetchReader = new PrefetchReader( files );
        PrefetchReader.setEnableLog( true );
        prefetchReader.start();

        long cached = 0;
        
        while( prefetchReader.pendingPages.size() > 0 || prefetchReader.cachedPages.size() > 0 ) {

            PrefetchReader.PageEntry page = prefetchReader.cachedPages.take();

            System.out.printf( "found cached: %s\n", page );

            cached += page.length;

            if ( cached >= prefetchReader.getCapacity() )
                break;
            
        }
        
        prefetchReader.close();

    }

    public static void main( String[] args ) throws Exception {
        runTests();
    }

}