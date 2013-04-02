package peregrine.io.sstable;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.os.*;

/**
 */
public class Main {

    public static void main( String[] args ) throws Exception {

        // TODO: things to test:
        // - writer with NO keys
        // - writer where the nr keys == block size (to detect issues with close and rollover)
        
        MappedFileWriter writer = new MappedFileWriter( null, "/tmp/test.dat" );

        SSTableWriter table = new SSTableWriter( writer );

        table.write( StructReaders.hashcode( "adsf" ), StructReaders.wrap( true ) );

        table.close();
        
    }
    
}