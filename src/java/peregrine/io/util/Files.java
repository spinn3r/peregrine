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
package peregrine.io.util;

import java.io.*;

/**
 * Handles a facade on top of various bulk filesystem manipulation primitives.
 */
public class Files {

    /**
     * @see #remove(File)
     */
    public static void remove( String path ) {
        remove( new File( path ) );
    }

    /**
     * Recursive removal of the given file and any children.
     */
    public static void remove( File file ) {

        if ( ! file.exists() )
            return;

        if ( file.isDirectory() ) {
        
            File[] files = file.listFiles();
            
            for ( File current : files ) {

                if ( current.isDirectory() ) {
                    remove( current );
                } else { 
                    remove0( current );
                }

            }

        }
            
        remove0( file );

    }

    private static void remove0( File file ) {

        if ( ! file.delete() )
            throw new RuntimeException( "Unable to delete: " + file.getPath() );

    }

    public static void purge( String path ) {
        purge( new File( path ) );
    }

    /**
     * Purge all files in the given directory but keep the given directory
     * itself.
     */
    public static void purge( File file ) {

        if ( file.isDirectory() ) {
        
            File[] files = file.listFiles();
            
            for ( File current : files ) {
                remove( current );                
            }

        }

    }
    
    /**
     * Recursive 
     */
    public static void removeChildren( File file ) {

        if ( ! file.exists() )
            return;

        File[] files = file.listFiles();
        
        for ( File current : files ) {

            if ( current.isDirectory() ) {
                remove( current );
            } else { 
                remove0( current );
            }

        }

        remove0( file );

    }

    public static void mkdirs( String path ) throws IOException {

        File file = new File( path );
        
        if ( file.exists() ) {

            if ( file.isDirectory() )
                return; /* we're done */
            else
                throw new IOException( "Path exist and is a regular file: " + path );

        }
        
        if ( new File( path ).mkdirs() == false ) {
            throw new IOException( "Unable to make directory: " + path );
        }
        
    }

    /**
     * Make a given directory readable and writable and optionally recurse all
     * children.
     */
    public static void setReadableAndWritable( String path,
                                               boolean ownerOnly,
                                               boolean recursive ) throws IOException {

        File file = new File( path );

        if ( file.setReadable( true, ownerOnly ) == false ) {
            throw new IOException( "Unable to make readable: " + path );
        }

        if ( file.setWritable( true, ownerOnly ) == false ) {
            throw new IOException( "Unable to make writable: " + path );
        }

        if ( recursive ) {

            File[] files = file.listFiles();

            if ( files != null ) {
            
                for( File current : files ) {
                    setReadableAndWritable( current.getPath() , ownerOnly, recursive );
                }

            }
            
        }

    }
    
    /**
     * Read the file data as UTF8 string.
     */
    public static String toString( File file ) throws IOException {

        FileInputStream fis = null;

        try {
            
            fis = new FileInputStream( file );
            
            byte[] data = new byte[ (int)file.length() ];
            fis.read( data );
            
            return new String( data, "UTF8" );

        } finally {
            new Closer( fis ).close();
        }

    }
    
}
