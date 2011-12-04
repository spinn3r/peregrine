package peregrine.os;

import java.io.*;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public class mman {

    private static InterfaceDelegate delegate
        = (InterfaceDelegate)Native.loadLibrary( "c", InterfaceDelegate.class); 

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
    public static Pointer mmap( Pointer addr, long len, int prot, int flags, int fildes, long off ) {

        Pointer result = delegate.mmap( addr, len, prot, flags, fildes, off );

        if ( Pointer.nativeValue( result ) == -1 ) {
            throw new IOException( errno.strerror() );
        }

        return result;
        
    }

    public static int munmap( Pointer addr, long len ) {

        int result = delegate.munmap( addr, len );

        if ( result != 0 ) {
            throw new IOException( errno.strerror() );
        }

        return result;

    }

    interface InterfaceDelegate extends Library {
        Pointer mmap( Pointer addr, long len, int prot, int flags, int fildes, long off );
        int munmap( Pointer addr, long len );
    }

}
