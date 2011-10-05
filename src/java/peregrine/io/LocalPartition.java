package peregrine.io;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;
import peregrine.values.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class LocalPartition {

    public static String getFilenameForChunkID( int local_chunk_id ) {
        return String.format( "chunk%06d.dat" , local_chunk_id );
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
    
    public static List<File> getChunkFiles( Partition part,
                                            Host host,
                                            String path ) {

        String dir = Config.getPFSPath( part, host, path );

        return getChunkFiles( dir );

    }

    public static List<ChunkReader> getChunkReaders( Partition part,
                                                     Host host,
                                                     String path )
        throws IOException {
    
        List<File> chunks = LocalPartition.getChunkFiles( part, host, path );

        List<ChunkReader> chunkReaders = new ArrayList();
        
        for( File chunk : chunks ) {
            chunkReaders.add( new DefaultChunkReader( chunk ) );
        }

        return chunkReaders;
        
    }
    
}