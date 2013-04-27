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

        MappedSSTableWriter tableWriter = new MappedSSTableWriter( writer );

        tableWriter.write( StructReaders.hashcode( "adsf" ), StructReaders.wrap( true ) );

        tableWriter.close();

        System.out.printf( "trailer written: %s\n", tableWriter.trailer );
        System.out.printf( "fileInfo written: %s\n", tableWriter.fileInfo );

        for( DataBlock block : tableWriter.dataBlocks ) {
            System.out.printf( "DataBlock written: %s\n", block );
        }
        
        MappedFileReader reader = new MappedFileReader( null, "/tmp/test.dat" );

        MappedSSTableReader tableReader = new MappedSSTableReader( reader );

        System.out.printf( "trailer read: %s\n", tableReader.trailer );
        System.out.printf( "fileInfo read: %s\n", tableReader.fileInfo );

        for( DataBlock block : tableReader.dataBlocks ) {
            System.out.printf( "DataBlock read: %s\n", block );
        }

    }

}