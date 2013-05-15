/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package peregrine.os;

import peregrine.util.*;

import java.io.*;

import com.sun.jna.Pointer;

import com.spinn3r.log5j.*;

/**
 * Facade on top of mlock/mmap that opens up a file, mmaps it, then mlocks it
 * (optionally).
 */
public class FileMapper extends IdempotentCloser implements Closeable {

    private static final Logger log = Logger.getLogger();

    private Pointer pa;
    private long length;
    private File file;
    private FileDescriptor descriptor;

    private boolean lock = false;

    /**
     * Call mmap a file descriptor, then lock the pages with MAP_LOCKED.  This
     * will then prevent the pages from being swapped out due to VFS cache
     * pressure.
     * 
     * @param descriptor The file we should mmap and MAP_LOCKED
     * @param offset
     * @param length
     * @see #close()
     * @throws IOException
     */
    public FileMapper( File file, FileDescriptor descriptor, long offset, long length ) throws IOException {

    	log.debug( "mapping %s with length %,d", file, length );

        this.file = file;
    	this.descriptor = descriptor;
        this.length = length;
        
        int fd = Platform.getFd( descriptor );

        int prot   = mman.PROT_READ;
        int flags  = mman.MAP_SHARED;

        if ( lock ) {
            flags  = mman.MAP_SHARED | mman.MAP_LOCKED;
            log.debug( "mapping(+lock) %s with length %,d", file, length );
        }

        pa = mman.mmap( length, prot, flags, fd, offset );

        // even though technically we have specified MAP_LOCKED this isn't
        // supported on OpenSolaris or older Linux kernels (or OS X) so call
        // mlock as well.

        if ( lock ) {
            mman.mlock( pa, length );
        }
        
    }

    public long getAddress() {
        return Pointer.nativeValue( pa );
    }

    /**
     * Unlock a region of the file from zero to 'length' bytes without unlocking
     * the rest of the file.  This can be used to unlock pages we've already
     * read so the memory can be reclaimed by OS.
     */
    public void unlockRegion( long len ) throws IOException {
        
        mman.munlock( pa, len );
        
    }
    
    /**
     * Release this lock so that the memory can be returned to the OS if it
     * wants to us it.
     */
    @Override
    protected void doClose() throws IOException {

        if ( lock ) {
            log.debug( "munlocking %s to pa %s with length %,d", file, pa, length );
            mman.munlock( pa, length );
        }
            
        mman.munmap( pa, length );

    }

    public boolean getLock() { 
        return this.lock;
    }

    public void setLock( boolean lock ) { 
        this.lock = lock;
    }

}
