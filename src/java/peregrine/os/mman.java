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
package peregrine.os;

import java.io.*;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public class mman {

    public static final int PROT_READ   = 0x1;      /* Page can be read.  */
    public static final int PROT_WRITE  = 0x2;      /* Page can be written.  */
    public static final int PROT_EXEC   = 0x4;      /* Page can be executed.  */
    public static final int PROT_NONE   = 0x0;      /* Page can not be accessed.  */

    public static final int MAP_SHARED	= 0x01;		/* Share changes.  */
    public static final int MAP_PRIVATE	= 0x02;		/* Changes are private.  */
    
    public static final int MAP_LOCKED  = 0x02000;  /* Lock the mapping.  */

    // http://linux.die.net/man/2/mmap
    // http://www.opengroup.org/sud/sud1/xsh/mmap.htm
    // http://linux.die.net/include/sys/mman.h
    // http://linux.die.net/include/bits/mman.h

    // off_t = 8
    // size_t = 8
    public static Pointer mmap( long len, int prot, int flags, int fildes, long off )
        throws IOException {

        // we don't really have a need to change the recommended pointer.
        Pointer addr = new Pointer( 0 );
        
        Pointer result = Delegate.mmap( addr, len, prot, flags, fildes, off );
        
        if ( Pointer.nativeValue( result ) == -1 ) {
            throw new IOException( errno.strerror() );
        }

        return result;
        
    }

    public static int munmap( Pointer addr, long len )
        throws IOException {

        int result = Delegate.munmap( addr, len );

        if ( result != 0 ) {
            throw new IOException( errno.strerror() );
        }

        return result;

    }

    public static void mlock( Pointer addr, long len )
        throws IOException {

        if ( Delegate.mlock( addr, len ) != 0 ) {
            throw new IOException( errno.strerror() );
        }

    }

    /**
     * Unlock the given region, throw an IOException if we fail.
     */
    public static void munlock( Pointer addr, long len )
        throws IOException {

        if ( Delegate.munlock( addr, len ) != 0 ) {
            throw new IOException( errno.strerror() );
        }

    }

    static class Delegate {
    
        public static native Pointer mmap( Pointer addr, long len, int prot, int flags, int fildes, long off );
        public static native int munmap( Pointer addr, long len );
        
        public static native int mlock( Pointer addr, long len );
        public static native int munlock( Pointer addr, long len );
        
        static {
            Native.register( "c" );
        }

    }

    public static void main( String[] args ) throws Exception {

        String path = args[0];

        File file = new File( path );
        FileInputStream in = new FileInputStream( file );
        int fd = peregrine.os.Native.getFd( in.getFD() );

        // mmap a large file... 
        Pointer addr = mmap( file.length(), PROT_READ, mman.MAP_SHARED | mman.MAP_LOCKED, fd, 0 );

        // try to mlock it directly
        mlock( addr, file.length() );
        munlock( addr, file.length() );
        
        munmap( addr, file.length() );
        
    }
                            
}
