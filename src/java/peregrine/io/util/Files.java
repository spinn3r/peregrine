package peregrine.io.util;

import java.io.*;

public class Files {
    
    public static void remove( String path ) {
        remove( new File( path ) );
    }

    public static void remove( File file ) {

        if ( ! file.exists() )
            return;

        File[] files = file.listFiles();
        
        for ( File current : files ) {

            if ( current.isDirectory() == false ) {
                current.delete();
            } else {
                remove( current );
            }
            
        }
        
    }

    /**
     * Read the file data as an ISO-8601 string.
     */
    public static String toString( File file ) throws IOException {

        FileInputStream fis = new FileInputStream( file );

        byte[] data = new byte[ (int)file.length() ];
        fis.read( data );

        return new String( data, "ISO-8601" );

    }
    
}