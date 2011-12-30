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

import com.sun.jna.Pointer;

import com.spinn3r.log5j.*;

public class MemLock implements Closeable {

    private static final Logger log = Logger.getLogger();

    private Pointer pa;
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
     * @see #close()
     * @throws IOException
     */
    public MemLock( File file, FileDescriptor descriptor, long offset, long length ) throws IOException {

    	log.info( "Going to mlock %s", file );

        this.file = file;
    	this.descriptor = descriptor;
        this.length = length;
        
        int fd = Native.getFd( descriptor );
        
        pa = mman.mmap( length, mman.PROT_READ, mman.MAP_SHARED | mman.MAP_LOCKED, fd, offset );

        // even though technically we have specified MAP_LOCKED this isn't
        // supported on OpenSolaris or older Linux kernels (or OS X).
        
        mman.mlock( pa, length );
        
    }

    /**
     * Release this lock so that the memory can be returned to the OS if it
     * wants to us it.
     */
    @Override
    public void close() throws IOException {

        String desc = String.format( "Releasing lock %s to pa: %s ... ", file, pa );
        
        log.info( "%s ...", desc );

        mman.munlock( pa, length );
        mman.munmap( pa, length );

        log.info( "%s ... done", desc );

    }

}
