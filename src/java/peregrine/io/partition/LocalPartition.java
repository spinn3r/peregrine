package peregrine.io.partition;

import java.io.*;
import java.util.*;

import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.chunk.*;

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
    
    public static List<ChunkReader> getChunkReaders( Config config,
                                                     Partition part,
                                                     String path )
        throws IOException {
    
        List<File> chunks = LocalPartition.getChunkFiles( config, part, path );

        List<ChunkReader> chunkReaders = new ArrayList();
        
        for( File chunk : chunks ) {
            chunkReaders.add( new DefaultChunkReader( chunk ) );
        }

        return chunkReaders;
        
    }

    public static List<File> getChunkFiles( Config config,
                                            Partition part,
                                            String path ) {

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