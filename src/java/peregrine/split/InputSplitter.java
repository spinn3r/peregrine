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
package peregrine.split;

import java.io.*;
import java.util.*;

import peregrine.io.*;
import peregrine.config.*;
import peregrine.util.*;
import peregrine.worker.*;
import peregrine.controller.*;

import com.spinn3r.log5j.Logger;

/**
 * Take large files and break them up into input splits.  We take an input file,
 * seek to the next offset, then find the point within that file where a record
 * ends.
 * 
 * <p> Right now this splits on the Wikipedia import boundaries.  In the future
 * we should make boundary detection a plugin based on file format.
 */
public class InputSplitter {

    private static final Logger log = Logger.getLogger();

    public static final int SPLIT_SIZE = 134217728; /* 2^27 ... 100MB splits */
    
    private int split_size = SPLIT_SIZE;

    private File file = null;

    private RandomAccessFile raf = null;

    private RecordFinder finder = null;
    
    private List<InputSplit> splits = new ArrayList();

    public InputSplitter( String path, RecordFinder finder ) throws IOException {
        this( path, finder, SPLIT_SIZE );
    }

    public InputSplitter( String path, RecordFinder finder, int split_size ) throws IOException {

        this.file = new File( path );
        this.finder = finder;
        this.split_size = split_size;
        this.raf = new RandomAccessFile( file, "r" );

        long length = file.length();
        long offset = 0;

        while ( offset < length ) {

            long end = offset + split_size;

            if ( end > length ) {
                end = length - 1;
                registerInputSplit( offset, end );
                break;
            }

            InputFileReader current = new InputFileReader( raf, offset, end );

            end = finder.findRecord( current, end );

            registerInputSplit( offset, end );

            offset = end + 1;
            
        }

    }

    public List<InputSplit> getInputSplits() {
        return splits;
    }
    
    private void registerInputSplit( long start, long end ) {

        InputSplit split = new InputSplit( start, end );
        log.info( "Found split: %s", split );
        
        splits.add( split );

    }
    
    private char read( long pos ) throws IOException {
        raf.seek( pos );
        return (char)raf.read();
    }
    
    public static void main( String[] args ) throws Exception {

        String path = args[0];
        InputSplitter splitter = new InputSplitter( path, new ExampleRecordFinder(), 128000 );

        List<InputSplit> result = splitter.getInputSplits();

        System.out.printf( "Found %,d splits\n", result.size() );
        
    }

}

class ExampleRecordFinder implements RecordFinder {

    @Override
    public long findRecord( InputFileReader reader, long pos ) throws IOException {

        while( true ) {

            if ( reader.read(pos - 0) == '(' &&
                 reader.read(pos - 1) == ',' &&
                 reader.read(pos - 2) == ')' ) {
                
                return pos - 1;
                
            }

            // go back one character now.
            --pos;
            
        }

    }

}