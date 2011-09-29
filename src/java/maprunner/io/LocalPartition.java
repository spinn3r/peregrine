package maprunner.io;

import java.io.*;
import java.util.*;

import maprunner.*;
import maprunner.util.*;
import maprunner.keys.*;
import maprunner.values.*;

/**
 * Write to a logical partition which is a stream of chunk files.... 
 */
public class LocalPartition {

    public static String getFilenameForChunkID( int local_chunk_id ) {
        return String.format( "chunk%06d.dat" , local_chunk_id );
    }

    public static List<File> getChunkFiles( Partition part, Host host, String path ) {

        List<File> files = new ArrayList();
        
        String dir = Config.getDFSPath( part, host, path );

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

}