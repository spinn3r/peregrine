package peregrine.perf;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class DiskPerf {

    public static void sync() throws Exception {

        System.out.printf( "sync..." );
        
        int result = Runtime.getRuntime().exec( "sync") .waitFor();

        if ( result != 0 )
            throw new Exception( "sync failed" );

        System.out.printf( "done\n" );
        
    }

    public static void dropCaches() throws Exception {

        System.out.printf( "Dropping caches..." );
        FileOutputStream fos = new FileOutputStream( "/proc/sys/vm/drop_caches" );
        fos.write( (byte)'3' );
        fos.close();
        System.out.printf( "done\n" );

    }

    public static void remove( String path ) {
        remove( new File( path ) );
    }

    public static void remove( File file ) {

        if ( ! file.exists() )
            return;

        File[] files = file.listFiles();
        
        for ( File current : files ) {

            if ( current.isDirectory() == false ) {
                System.out.printf( "Deleting: %s\n", current.getPath() );
                current.delete();
            } else {
                remove( current );
            }
            
        }
        
    }

    public static List<String> find( String path ) {
        return find( new File( path ), null );
    }

    public static List<String> find( File file, List<String> result ) {

        if ( result == null )
            result = new ArrayList();
        
        File[] files = file.listFiles();
        
        for ( File current : files ) {

            if ( current.isDirectory() == false ) {
                result.add( current.getPath() );
            } else {
                find( current, result );
            }
            
        }

        return result;

    }

}