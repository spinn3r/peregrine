package peregrine.os;

import com.sun.jna.Library;
import com.sun.jna.Native;

public class fadvise {

    private static InterfaceDelegate delegate
        = (InterfaceDelegate)Native.loadLibrary( "c", InterfaceDelegate.class); 

    /**
     * 
     * POSIX_FADV_NORMAL       No further special treatment.  
     * POSIX_FADV_RANDOM       Expect random page references.  
     * POSIX_FADV_SEQUENTIAL   Expect sequential page references.  
     * POSIX_FADV_WILLNEED     Will need these pages.  
     * POSIX_FADV_DONTNEED     Don't need these pages.  
     * POSIX_FADV_NOREUSE      Data will be accessed once.  
     * 
     * Allows an application to to tell the kernel how it expects to use a file
     * handle, so that the kernel can choose appropriate read-ahead and caching
     * techniques for access to the corresponding file. This is similar to the
     * POSIX version of the madvise system call, but for file access instead of
     * memory access. The sys_fadvise64() function is obsolete and corresponds
     * to a broken glibc API, sys_fadvise64_64() is the fixed version. The
     * following are the values for the advice parameter:
     * 
     * FADV_NORMAL
     * 
     * No special treatment.
     * 
     * FADV_RANDOM
     * 
     * Expect page references in random order.
     * 
     * FADV_SEQUENTIAL
     * 
     * Expect page references in sequential order.
     * 
     * FADV_WILLNEED
     * 
     * Expect access in the near future.
     * 
     * FADV_DONTNEED
     * 
     * Do not expect access in the near future. Subsequent access of pages in
     * this range will succeed, but will result either in reloading of the
     * memory contents from the underlying mapped file or zero-fill-in-demand
     * pages for mappings without an underlying file.
     * 
     * FADV_NOREUSE
     * 
     * Access data only once.
     * 
     */
    public static int posix_fadvise(int fd, long offset, long len, int advice ) {
        return delegate.posix_fadvise(fd, offset, len, advice )
    }

    interface InterfaceDelegate extends Library {
        int posix_fadvise(int fd, long offset, long len, int advice );
    }

}