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
     * Recursive removal of all files on the given path.
     */
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
