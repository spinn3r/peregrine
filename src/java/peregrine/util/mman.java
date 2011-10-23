package peregrine.util;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Structure;

public class mman {

    private static InterfaceDelegate delegate
        = (InterfaceDelegate)Native.loadLibrary( "c", InterfaceDelegate.class); 

    public static final int PROT_READ   = 0x1;      /* Page can be read.  */
    public static final int PROT_WRITE  = 0x2;      /* Page can be written.  */
    public static final int PROT_EXEC   = 0x4;      /* Page can be executed.  */
    public static final int PROT_NONE   = 0x0;      /* Page can not be accessed.  */

    public static final int MAP_LOCKED  = 0x02000;  /* Lock the mapping.  */

    // http://linux.die.net/man/2/mmap
    // http://www.opengroup.org/sud/sud1/xsh/mmap.htm
    // http://linux.die.net/include/sys/mman.h
    // http://linux.die.net/include/bits/mman.h

    public static long mmap( long addr, long len, int prot, int flags, int fildes, long off ) {
        return delegate.mmap( addr, len, prot, flags, fildes, off );
    }

    interface InterfaceDelegate extends Library {
        long mmap( long addr, long len, int prot, int flags, int fildes, long off );
    }

}
