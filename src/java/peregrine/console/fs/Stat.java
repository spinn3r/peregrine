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
package peregrine.console.fs;

import peregrine.*;
import peregrine.util.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.util.*;
import peregrine.io.sstable.*;

import java.io.*;
import java.util.*;

/**
 * Cat a file on the filesytem.  This reads from a local PFS directory file
 * (someday this may be globally via HTTP across the entire filesystem) and cats
 * the file to standard out.
 * 
 * <p>
 * 
 * This can be used for debug purposes so that one can understand what is stored
 * on the filesystem without having to write additional map reduce jobs.
 */
public class Stat {

    public static void help() {

        System.out.printf( "Usage: fsstat [FILE]\n" );
        
    }
    
    public static void main( String[] args ) throws Exception {

        Getopt getopt = new Getopt( args );

        if ( getopt.getBoolean( "help" ) ) {
            help();
            System.exit( 1 );
        }
        
        String render  = getopt.getString( "render" , "base64" );
        int limit      = getopt.getInt( "limit", -1 );

        //TODO: go over ALL paths specified so the user can cat multiple files.
        String path = getopt.getValues().get( 0 );

        DefaultChunkReader reader = new DefaultChunkReader( new File( path ) );

        //System.out.printf( "count: %,d\n", reader.getTrailer().count() );
        //System.out.printf( "record usage: %,d\n", reader.getTrailer().count() );

        System.out.printf( "trailer: \n" );
        System.out.printf( "    %s\n", reader.getTrailer() );
        System.out.printf( "file info: \n" );
        System.out.printf( "    %s\n", reader.getFileInfo() );
        System.out.printf( "data blocks: \n" );

        for( DataBlock db : reader.getDataBlocks() ) {

            System.out.printf( "    %s\n", db );
            
        }

    }

}

