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
package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.chunk.*;
import peregrine.os.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class LocalPartition {

    public static String getFilenameForChunkID( int local_chunk_id ) {
        return String.format( "chunk%06d.dat" , local_chunk_id );
    }

    public static List<DefaultChunkReader> getChunkReaders( Config config,
                                                            Partition part,
                                                            String path )
        throws IOException {
    
        List<File> chunks = LocalPartition.getChunkFiles( config, part, path );

        List<DefaultChunkReader> result = new ArrayList();
        
        for( File chunk : chunks ) {
            result.add( new DefaultChunkReader( config, chunk ) );
        }

        return result;

    }

    public static List<File> getChunkFiles( String dir ) {

        List<File> files = new ArrayList();

        for( int i = 0; i < Integer.MAX_VALUE; ++i ) {

            String name = getFilenameForChunkID( i );

            File chunk = new File( dir, name );

            if ( chunk.exists() ) {
                files.add( chunk);
            } else {
                break;
            }
                
        }

        return files;

    }

    public static List<File> getChunkFiles( Config config,
                                            Partition part,
                                            String path ) {

        if ( config == null )
            throw new NullPointerException( "config" );
        
        String dir = config.getPath( part, path );

        return getChunkFiles( dir );

    }

    public static File getChunkFile( Config config,
                                     Partition part,
                                     String path,
                                     int chunk_id ) {

        String local = config.getPath( part, path );

        String chunk_name = LocalPartition.getFilenameForChunkID( chunk_id );

        return new File( local, chunk_name );

    }

}
