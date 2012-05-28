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

    /**
     * 
     * 
     * mmap() creates a new mapping in the virtual address space of the calling
     * process. The starting address for the new mapping is specified in
     * addr. The length argument specifies the length of the mapping.
     * 
     * If addr is NULL, then the kernel chooses the address at which to create
     * the mapping; this is the most portable method of creating a new
     * mapping. If addr is not NULL, then the kernel takes it as a hint about
     * where to place the mapping; on Linux, the mapping will be created at a
     * nearby page boundary. The address of the new mapping is returned as the
     * result of the call.
     * 
     * The contents of a file mapping (as opposed to an anonymous mapping; see
     * MAP_ANONYMOUS below), are initialized using length bytes starting at
     * offset offset in the file (or other object) referred to by the file
     * descriptor fd. offset must be a multiple of the page size as returned by
     * sysconf(_SC_PAGE_SIZE).
     * 
     * The prot argument describes the desired memory protection of the mapping
     * (and must not conflict with the open mode of the file). It is either
     * PROT_NONE or the bitwise OR of one or more of the following flags:
     * 
     * PROT_EXEC
     * 
     * Pages may be executed.
     * 
     * PROT_READ Pages may be read.
     * 
     * PROT_WRITE
     * 
     * Pages may be written.
     * 
     * PROT_NONE
     * 
     * Pages may not be accessed.
     * 
     * The flags argument determines whether updates to the mapping are visible
     * to other processes mapping the same region, and whether updates are
     * carried through to the underlying file. This behavior is determined by
     * including exactly one of the following values in flags:
     * 
     * MAP_SHARED
     * 
     * Share this mapping. Updates to the mapping are visible to other processes
     * that map this file, and are carried through to the underlying file. The
     * file may not actually be updated until msync(2) or munmap() is called.
     * 
     * MAP_PRIVATE
     * 
     * Create a private copy-on-write mapping. Updates to the mapping are not
     * visible to other processes mapping the same file, and are not carried
     * through to the underlying file. It is unspecified whether changes made to
     * the file after the mmap() call are visible in the mapped region.
     * 
     * Both of these flags are described in POSIX.1-2001.
     * 
     * In addition, zero or more of the following values can be ORed in flags:
     * 
     * MAP_32BIT (since Linux 2.4.20, 2.6)
     * 
     * Put the mapping into the first 2 Gigabytes of the process address
     * space. This flag is only supported on x86-64, for 64-bit programs. It was
     * added to allow thread stacks to be allocated somewhere in the first 2GB
     * of memory, so as to improve context-switch performance on some early
     * 64-bit processors. Modern x86-64 processors no longer have this
     * performance problem, so use of this flag is not required on those
     * systems. The MAP_32BIT flag is ignored when MAP_FIXED is set.
     * 
     * MAP_ANON
     * 
     * Synonym for MAP_ANONYMOUS. Deprecated.
     * 
     * MAP_ANONYMOUS
     * 
     * The mapping is not backed by any file; its contents are initialized to
     * zero. The fd and offset arguments are ignored; however, some
     * implementations require fd to be -1 if MAP_ANONYMOUS (or MAP_ANON) is
     * specified, and portable applications should ensure this. The use of
     * MAP_ANONYMOUS in conjunction with MAP_SHARED is only supported on Linux
     * since kernel 2.4.
     * 
     * MAP_DENYWRITE
     * 
     * This flag is ignored. (Long ago, it signaled that attempts to write to
     * the underlying file should fail with ETXTBUSY. But this was a source of
     * denial-of-service attacks.)
     * 
     * MAP_EXECUTABLE
     * 
     * This flag is ignored.
     * 
     * MAP_FILE
     * 
     * Compatibility flag. Ignored.
     * 
     * MAP_FIXED
     * 
     * Don't interpret addr as a hint: place the mapping at exactly that
     * address. addr must be a multiple of the page size. If the memory region
     * specified by addr and len overlaps pages of any existing mapping(s), then
     * the overlapped part of the existing mapping(s) will be discarded. If the
     * specified address cannot be used, mmap() will fail. Because requiring a
     * fixed address for a mapping is less portable, the use of this option is
     * discouraged.
     * 
     * MAP_GROWSDOWN
     * 
     * Used for stacks. Indicates to the kernel virtual memory system that the
     * mapping should extend downward in memory.
     * 
     * MAP_HUGETLB (since Linux 2.6.32)
     * 
     * Allocate the mapping using "huge pages." See the kernel source file
     * Documentation/vm/hugetlbpage.txt for further information.
     * 
     * MAP_LOCKED (since Linux 2.5.37)
     * 
     * Lock the pages of the mapped region into memory in the manner of
     * mlock(2). This flag is ignored in older kernels.
     * 
     * MAP_NONBLOCK (since Linux 2.5.46)
     * 
     * Only meaningful in conjunction with MAP_POPULATE. Don't perform
     * read-ahead: only create page tables entries for pages that are already
     * present in RAM. Since Linux 2.6.23, this flag causes MAP_POPULATE to do
     * nothing. One day the combination of MAP_POPULATE and MAP_NONBLOCK may be
     * reimplemented.
     * 
     * MAP_NORESERVE
     * 
     * Do not reserve swap space for this mapping. When swap space is reserved,
     * one has the guarantee that it is possible to modify the mapping. When
     * swap space is not reserved one might get SIGSEGV upon a write if no
     * physical memory is available. See also the discussion of the file
     * /proc/sys/vm/overcommit_memory in proc(5). In kernels before 2.6, this
     * flag only had effect for private writable mappings.
     * 
     * MAP_POPULATE (since Linux 2.5.46) Populate (prefault) page tables for a
     * mapping. For a file mapping, this causes read-ahead on the file. Later
     * accesses to the mapping will not be blocked by page faults. MAP_POPULATE
     * is only supported for private mappings since Linux 2.6.23.
     * 
     * MAP_STACK (since Linux 2.6.27)
     * 
     * Allocate the mapping at an address suitable for a process or thread
     * stack. This flag is currently a no-op, but is used in the glibc threading
     * implementation so that if some architectures require special treatment
     * for stack allocations, support can later be transparently implemented for
     * glibc.
     * 
     * MAP_UNINITIALIZED (since Linux 2.6.33)
     * 
     * Don't clear anonymous pages. This flag is intended to improve performance
     * on embedded devices. This flag is only honored if the kernel was
     * configured with the CONFIG_
     * 
     * MMAP_ALLOW_UNINITIALIZED option. Because of the security implications,
     * that option is normally enabled only on embedded devices (i.e., devices
     * where one has complete control of the contents of user memory).
     * 
     * Of the above flags, only MAP_FIXED is specified in POSIX.1-2001. However,
     * most systems also support MAP_ANONYMOUS (or its synonym MAP_ANON).
     * 
     * Some systems document the additional flags MAP_AUTOGROW, MAP_AUTORESRV,
     * MAP_COPY, and MAP_LOCAL.
     * 
     * Memory mapped by mmap() is preserved across fork(2), with the same
     * attributes.
     * 
     * A file is mapped in multiples of the page size. For a file that is not a
     * multiple of the page size, the remaining memory is zeroed when mapped,
     * and writes to that region are not written out to the file. The effect of
     * changing the size of the underlying file of a mapping on the pages that
     * correspond to added or removed regions of the file is unspecified.
     *
     * 
     * Return Value
     * 
     * On success, mmap() returns a pointer to the mapped area. On error, the
     * value MAP_FAILED (that is, (void *) -1) is returned, and errno is set
     * appropriately. On success, munmap() returns 0, on failure -1, and errno
     * is set (probably to EINVAL).
     * 
     */
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

    /**
     * The munmap() system call deletes the mappings for the specified address
     * range, and causes further references to addresses within the range to
     * generate invalid memory references. The region is also automatically
     * unmapped when the process is terminated. On the other hand, closing the
     * file descriptor does not unmap the region.
     * 
     * The address addr must be a multiple of the page size. All pages
     * containing a part of the indicated range are unmapped, and subsequent
     * references to these pages will generate SIGSEGV. It is not an error if
     * the indicated range does not contain any mapped pages.
     */
    public static int munmap( Pointer addr, long len )
        throws IOException {

        int result = Delegate.munmap( addr, len );

        if ( result != 0 ) {
            throw new IOException( errno.strerror() );
        }

        return result;

    }

    /**
     * mlock() and mlockall() respectively lock part or all of the calling
     * process's virtual address space into RAM, preventing that memory from
     * being paged to the swap area. munlock() and munlockall() perform the
     * converse operation, respectively unlocking part or all of the calling
     * process's virtual address space, so that pages in the specified virtual
     * address range may once more to be swapped out if required by the kernel
     * memory manager. Memory locking and unlocking are performed in units of
     * whole pages.
     * 
     * mlock() and munlock()
     * 
     * mlock() locks pages in the address range starting at addr and continuing
     * for len bytes. All pages that contain a part of the specified address
     * range are guaranteed to be resident in RAM when the call returns
     * successfully; the pages are guaranteed to stay in RAM until later
     * unlocked.
     * 
     * munlock() unlocks pages in the address range starting at addr and
     * continuing for len bytes. After this call, all pages that contain a part
     * of the specified memory range can be moved to external swap space again
     * by the kernel.
     * 
     */
    public static void mlock( Pointer addr, long len )
        throws IOException {

        if ( Delegate.mlock( addr, len ) != 0 ) {
            throw new IOException( String.format( "Unable to mlock %s with length %s" , addr, len ), new PlatformException() );
        }

    }

    /**
     * Unlock the given region, throw an IOException if we fail.
     */
    public static void munlock( Pointer addr, long len )
        throws IOException {

        int result = Delegate.munlock( addr, len ); 
        
        if ( result != 0 ) {
            throw new IOException( "Result was: " + result + ": " + errno.strerror() );
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
        int fd = Platform.getFd( in.getFD() );

        // mmap a large file... 
        Pointer addr = mmap( file.length(), PROT_READ, mman.MAP_SHARED | mman.MAP_LOCKED, fd, 0 );

        // try to mlock it directly
        mlock( addr, file.length() );
        munlock( addr, file.length() );
        
        munmap( addr, file.length() );
        
    }
                            
}
