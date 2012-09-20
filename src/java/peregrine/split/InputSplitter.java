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
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import peregrine.app.wikirank.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.io.util.*;
import peregrine.util.*;
import peregrine.worker.*;

import org.jboss.netty.buffer.*;

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

    private RecordFinder finder = null;
    
    private List<InputSplit> splits = new ArrayList();

    private RandomAccessFile raf = null;

    private FileChannel channel = null;

    public InputSplitter( String path, RecordFinder finder ) throws IOException {
        this( path, finder, SPLIT_SIZE );
    }

    public InputSplitter( String path, RecordFinder finder, int split_size ) throws IOException {

        //TODO: if the input is a directory, process every file.

        File[] files = getFiles( path );
        
        for( File file : files ) {
            
            try {

                this.finder = finder;
                this.split_size = split_size;

                RandomAccessFile raf = new RandomAccessFile( file, "r" );
                FileChannel channel = raf.getChannel();

                long length = file.length();
                long offset = 0;

                while ( offset < length ) {

                    long end = offset + split_size;

                    if ( end > length ) {
                        end = length - 1;
                        registerInputSplit( file, offset, end );
                        break;
                    }

                    InputFileReader current = new InputFileReader( raf, offset, end );

                    end = finder.findRecord( current, end );

                    registerInputSplit( file, offset, end );

                    offset = end + 1;
                    
                }

            } finally {
                new Closer( raf, channel ).close();
            }

        }
            
    }

    /**
     * If we are given a file, return it, if a directory, return the files in
     * that directory.
     */
    private File[] getFiles( String path ) {
        
        File test = new File( path );
        
        if ( test.isDirectory() ) {
            return test.listFiles();
            
        } else {
            return new File[] { test };
        }
        
    }
    
    private void registerInputSplit( File file, long start, long end ) throws IOException {

        long length = end - start;
        
        FileInputStream fis = new FileInputStream( file );
        FileChannel channel = raf.getChannel();

        ByteBuffer buff = channel.map( FileChannel.MapMode.READ_ONLY, start , length );
        
        ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer( buff );

        InputSplit split = new InputSplit( start, end, channelBuffer, fis, channel );
        log.info( "Found split: %s", split );
        
        splits.add( split );

    }

    public List<InputSplit> getInputSplits() {
        return splits;
    }

    /**
     * Take the InputSplits and evenly divide them across partitions which would
     * process them...  
     */
    public Map<Partition,List<InputSplit>> getInputSplitsForPartitions( Config config ) {

        Map<Partition,List<InputSplit>> result = new HashMap();

        List<Partition> partitions = config.getMembership().getPartitions( config.getHost() );

        for( Partition part : partitions ) {
            result.put( part, new ArrayList() );
        }

        if ( partitions.size() == 0 )
            throw new RuntimeException( "no partitions for host: " + config.getHost() );
        
        int nr_splits_par_partition = splits.size() / partitions.size();

        List<InputSplit> partitionSplits = null;
        
        while( splits.size() != 0 ) {

            InputSplit split = splits.remove( 0 );

            if ( partitionSplits == null || partitionSplits.size() >= nr_splits_par_partition ) {

                if ( partitions.size() > 0 ) {
                
                    Partition part = partitions.remove( 0 );
                    partitionSplits = result.get( part );

                }
                
            }

            partitionSplits.add( split );
            
        }
        
        return result;
        
    }

    public List<InputSplit> getInputSplitsForPartitions( Config config, Partition partition ) {
        return getInputSplitsForPartitions( config ).get( partition );
    }

    public static void main( String[] args ) throws Exception {

        String path = args[0];
        InputSplitter splitter = new InputSplitter( path, new WikiRecordFinder(), 128000 );

        List<InputSplit> result = splitter.getInputSplits();

        System.out.printf( "Found %,d splits\n", result.size() );
        
    }

}
