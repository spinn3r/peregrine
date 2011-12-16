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
        config.initEnabledFeatures();
        
        MappedFile mappedFile = new MappedFile( config, file, "r" );

        StreamReader reader = mappedFile.getStreamReader();
        
        List<MappedFile> input = new ArrayList();
        input.add( mappedFile );
        
        PrefetchReader prefetchReader = new PrefetchReader( config, input );

        prefetchReader.setEnableLog( true );
        //prefetchReader.setCapacity( file.length() );
        //prefetchReader.start();

        long cached = 0;

        int read = 0;
        int width = 10;
        long length = file.length();
        
        while( true ) {

            if ( read + width > length )
                width = (int)(length - read);
            
            reader.read( width );
            
            read += width;

            if ( read >= length )
                break;
            
        }

        // FIXME: make sure the right pages were read.
        
        prefetchReader.close();

    }

    // FIXME: build a test reading say 10 bytes at a time form the
    // StreamReader until we are a the end of the file.
    
    public static void main( String[] args ) throws Exception {
        runTests();
    }

}