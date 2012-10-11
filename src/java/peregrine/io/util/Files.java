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
import java.util.concurrent.atomic.*;
import java.lang.reflect.*;

import peregrine.os.*;

import com.spinn3r.log5j.*;

/**
 * Handles a facade on top of various bulk filesystem manipulation primitives.
 */
public class Files {

    private static final Logger log = Logger.getLogger();

    /**
     * @see #remove(File)
     */
    public static void remove( String path ) throws IOException {
        remove( new File( path ) );
    }

    /**
     * Recursive removal of the given file and any children.
     */
    public static void remove( File file ) throws IOException {
        
        if ( ! file.exists() ) {
            // the file is already deleted.  We don't need to do anything.
            return;
        }

        //TODO: migrate to using Recursively

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

    private static void remove0( File file ) throws IOException {

        if ( ! file.delete() )
            throw new IOException( "Unable to delete: " + file.getPath() );

    }

    public static void purge( String path ) throws IOException {
        purge( new File( path ) );
    }

    /**
     * Purge all files in the given directory but keep the given directory
     * itself.
     */
    public static void purge( File file ) throws IOException {

        //TODO: migrate to using Recursively

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
    public static void removeChildren( File file ) throws IOException {

        if ( ! file.exists() )
            return;

        //TODO: migrate to using Recursively
        
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

            File test = new File( file.getPath() );

            // TODO: technically we also have to look at the permissions and
            // chown them
            
            if ( test.isDirectory() )
                return;

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
    public static void setReadable( final String path,
                                    final boolean ownerOnly,
                                    final boolean recursive ) throws IOException {

        File file = new File( path );

        if ( file.setReadable( true, ownerOnly ) == false ) {
            throw new IOException( "Unable to make readable: " + path );
        }

        if ( recursive ) {

            new Recursively<IOException>( file ) {

                public void handle( File current ) throws IOException {
                    setReadable( current.getPath() , ownerOnly, recursive );
                }
                
            };

        }

    }

    /**
     * Make a given directory writable and optionally recurse all
     * children.
     */
    public static void setWritable( final String path,
                                    final boolean ownerOnly,
                                    final boolean recursive ) throws IOException {

        File file = new File( path );

        if ( file.setWritable( true, ownerOnly ) == false ) {
            throw new IOException( "Unable to make writable: " + path );
        }

        if ( recursive ) {

            new Recursively<IOException>( file ) {

                public void handle( File current ) throws IOException {
                    setWritable( current.getPath() , ownerOnly, recursive );
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

        setReadable( path, ownerOnly, recursive );
        setWritable( path, ownerOnly, recursive );
        
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

    /**
     * Compute the usage, in bytes, of the given directory.
     */
    public static long usage( File file ) throws IOException {

        if ( file.isDirectory() == false )
            return file.length();

        final AtomicLong result = new AtomicLong();
        
        new Recursively<IOException>( file ) {

            public void handle( File current ) throws IOException {
                result.getAndAdd( current.length() );
            }
            
        };

        return result.get();
        
    }
    
}

/**
 * Handles listing the files, looking to see if they are directories, and if so
 * handling each directory.
 */
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

