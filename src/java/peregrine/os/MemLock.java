package peregrine.os;

import java.io.*;

import com.spinn3r.log5j.*;

public class MemLock {

    private static final Logger log = Logger.getLogger();

    private long pa;
    private long length;
    private File file;
    private FileDescriptor descriptor;
    
    /**
     * Call mmap a file descriptor, then lock the pages with MAP_LOCKED.  This 
     * will then prevent the pages from being swapped out due to VFS cache 
     * pressure.  
     * 
     * @param descriptor The file we should mmap and MAP_LOCKED
     * @param offset
     * @param length
     * @see #release()
     * @throws IOException
     */
    public MemLock( File file, FileDescriptor descriptor, long offset, long length ) throws IOException {

    	log.info( "Going to mlock %s", file );
    	
    	this.descriptor = descriptor;
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

    	log.info( "Releasing lock %s", file );
    		
        if ( mman.munmap( pa, length ) != 0 ) {
            throw new IOException( errno.strerror() );
        }

    }

}