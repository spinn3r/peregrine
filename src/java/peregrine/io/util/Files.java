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
import java.lang.reflect.*;

import peregrine.os.*;

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
     * Remove all files in the given directory but keep the parent directory.
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
        mkdirs( new File( path ) );
    }

    // public static void mkdirs( File file ) throws IOException {

    //     if ( file.exists() ) {
        
    //         if ( file.isDirectory() )
    //             return; /* we're done */

    //         throw new IOException( "File exists (not a directory): " + file.getPath() );
            
    //     }

    //     if ( file.mkdirs() == false ) {
    //         throw new IOException( "Unable to make directory: " + file.getPath() );
    //     }
            
    // }

    public static void mkdirs( File file ) throws IOException {

        file = file.getCanonicalFile();
        
        File parent = file.getParentFile();
        
        if ( parent != null ) {
            mkdirs( parent );
        }
        
        if ( file.exists() ) {
        
            if ( file.isDirectory() ) 
                return; /* we're done */

            throw new IOException( "File exists (not a directory): " + file.getPath() );
            
        }

        // NOTE: do not use java.io.File.mkdirs because it does not tell us WHY
        // we weren't able to make the directory (permissions, disk space, etc).
        // JDK 1.7 has a workaround for this but might as well just use JNA for
        // now.
        
        try {

            unistd.mkdir( file.getPath(), unistd.ACCESSPERMS );

        } catch ( IOException e ) {

            // there is a race condition where another thread could have created
            // the directory but that is fine with us... 
            while( true ) {

                Throwable cause = e.getCause();

                if ( cause == null )
                    break;

                if ( cause instanceof PlatformException ) {

                    PlatformException pe = (PlatformException)cause;

                    if ( pe.getErrno() == errno.EEXIST ) {

                        File test = new File( file.getPath() );

                        if ( test.isDirectory() )
                            return;
                        
                    }
                    
                }
                
            }
            
            String msg = String.format( "Unable to make directory '%s': %s",
                                        file.getPath(),
                                        e.getMessage() );
            
            throw new IOException( msg, e );
            
        }
        
    }

    /**
     * Init a given data directory.  Create the path via mkdirs, set ownership,
     * and also make the files writable.
     */
    public static void initDataDir( String path , String owner ) throws IOException {

        mkdirs( path );

        chown( new File( path ), owner, true );

        setReadableAndWritable( path, false, true );
        
    }

    /**
     * chown the given path to the given owner (and use their group).
     */
    public static void chown( File file ,
                              String owner,
                              boolean recursive ) throws IOException {

        pwd.Passwd passwd = pwd.getpwnam( owner );

        if ( passwd == null )
            throw new IOException( "Owner is invalid user: " + owner );
        
        chown( file, passwd, recursive );

    }

    public static void chown( final File file ,
                              final pwd.Passwd owner,
                              final boolean recursive ) throws IOException {
        
        unistd.chown( file.getPath(), owner.uid, owner.gid );

        if ( recursive ) {

            new Recursively<IOException>( file ) {

                public void handle( File current ) throws IOException {
                    chown( current, owner, recursive );
                }
                
            };
            
        }

    }
    
    /**
     * Make a given directory readable and writable and optionally recurse all
     * children.
     */
    public static void setReadableAndWritable( final String path,
                                               final boolean ownerOnly,
                                               final boolean recursive ) throws IOException {

        File file = new File( path );

        if ( file.setReadable( true, ownerOnly ) == false ) {
            throw new IOException( "Unable to make readable: " + path );
        }

        if ( file.setWritable( true, ownerOnly ) == false ) {
            throw new IOException( "Unable to make writable: " + path );
        }

        if ( recursive ) {

            new Recursively<IOException>( file ) {

                public void handle( File current ) throws IOException {
                    setReadableAndWritable( current.getPath() , ownerOnly, recursive );
                }
                
            };

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

abstract class Recursively <T extends Throwable> {

    public Recursively( File file ) throws T {

        File[] files = file.listFiles();

        if ( files != null ) {
        
            for( File current : files ) {

                handle( current );

                if ( current.isDirectory() ) {
                    handle( current );
                }

            }

        }

    }

    /**
     * Handle a file while we're recursing.
     */
    public abstract void handle( File file ) throws T;

}

