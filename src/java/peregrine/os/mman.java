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
    public static Pointer mmap( long len, int prot, int flags, int fildes, long off )
        throws IOException {

        // we don't really have a need to change the recommended pointer.
        Pointer addr = new Pointer( 0 );
        
        Pointer result = delegate.mmap( addr, len, prot, flags, fildes, off );

        if ( Pointer.nativeValue( result ) == -1 ) {
            throw new IOException( errno.strerror() );
        }

        return result;
        
    }

    public static int munmap( Pointer addr, long len )
        throws IOException {

        int result = delegate.munmap( addr, len );

        if ( result != 0 ) {
            throw new IOException( errno.strerror() );
        }

        return result;

    }

    public static void mlock( Pointer addr, long len )
        throws IOException {

        if ( delegate.mlock( addr, len ) != 0 ) {
            throw new IOException( errno.strerror() );
        }

    }

    public static void munlock( Pointer addr, long len )
        throws IOException {

        if ( delegate.munlock( addr, len ) != 0 ) {
            throw new IOException( errno.strerror() );
        }

    }

    interface InterfaceDelegate extends Library {
        Pointer mmap( Pointer addr, long len, int prot, int flags, int fildes, long off );
        int munmap( Pointer addr, long len );

        int mlock( Pointer addr, long len );
        int munlock( Pointer addr, long len );
    }

    public static void main( String[] args ) throws Exception {

        String path = args[0];

        File file = new File( path );
        FileInputStream in = new FileInputStream( file );
        int fd = peregrine.os.Native.getFd( in.getFD() );

        // mmap a large file... 
        Pointer addr = mmap( file.length(), PROT_READ, mman.MAP_SHARED | mman.MAP_LOCKED, fd, 0 );

        // try to mlock it directly
        //mlock( addr, file.length() );
        //munlock( addr, file.length() );
        
        munmap( addr, file.length() );
        
    }
                            
}
