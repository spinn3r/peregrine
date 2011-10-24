package peregrine.os;

import java.util.*;
import java.lang.reflect.*;
import java.io.*;

import java.nio.*;
import java.nio.channels.*;

import com.spinn3r.log5j.*;

public class MemLock {

    private static final Logger log = Logger.getLogger();

    private long pa;
    private long offset;
    private long length;
    
    public MemLock( FileDescriptor descriptor, long offset, long length ) throws IOException {

        this.offset = offset;
        this.length = length;
        
        int fd = Native.getFd( descriptor );
        
        this.pa = mman.mmap( 0, length, mman.PROT_READ, mman.MAP_SHARED | mman.MAP_LOCKED, fd, offset );

        if ( this.pa == -1 ) {
            throw new IOException( errno.strerror() );
        }

    }

    /**
     * Release this lock so that the memory can be returned to the OS if it
     * wants to us it.
     */
    public void release() throws IOException {

        if ( mman.munmap( pa, length ) != 0 ) {
            throw new IOException( errno.strerror() );
        }

    }

}