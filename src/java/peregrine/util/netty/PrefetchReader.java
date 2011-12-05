package peregrine.util.netty;

import java.io.*;
import java.util.*;

import peregrine.os.*;

import com.sun.jna.Pointer;

/**
 * Takes a given set of files and listens to a StreamReader and mlocks pages and
 * then requests that they be read.  Once the pages reads are complete, we evict
 * them from the page cache.
 * 
 */
public class PrefetchReader implements StreamReaderListener {

    public static long PAGE_SIZE = (long)Math.pow( 2, 17 );

    private List<PageEntry> pages = new ArrayList();

    private long offset = 0;

    /**
     * The amount of data that has been cached.  Offset must always be <=
     * cached.
     */ 
    private long cached = 0;
    
    public PrefetchReader() { }

    public PrefetchReader( List<MappedFile> files ) {

        List<FileMeta> filemeta = new ArrayList();
        
        for( MappedFile mappedFile : files ) {

            File file = mappedFile.getFile();

            filemeta.add( new FileMeta( file.getPath(), file.length(), mappedFile.getFd() ) );
        }

        init( filemeta );
        
    }

    protected void init( List<FileMeta> files ) {

        for( FileMeta file : files ) {

            long length = PAGE_SIZE;

            for( long offset = 0; offset < file.length; offset += PAGE_SIZE ) {

                if ( offset + length > file.length ) {
                    length = file.length - offset;
                } 

                pages.add( new PageEntry( file, offset, length ) );

                // adjust the length or next time around

                length = PAGE_SIZE - length;

                if ( length == 0 )
                    length = PAGE_SIZE;

            }
            
        }

    }

    @Override /* StreamReaderListener */
    public void onRead( int length ) {

        offset += length;
        
        if ( offset > cached && pages.size() > 0 ) {

            PageEntry entry = pages.remove( 0 );
            
            try {
                
                cache( entry );

            } catch ( IOException e ) {

                throw new RuntimeException( "Unable to prefetch file: " + entry, e );
                
            }

        }

    }

    /**
     * Cache a page on disk.  
     */
    private void cache( PageEntry pageEntry ) throws IOException {

        pageEntry.pa = mman.mmap( pageEntry.length,
                                  mman.PROT_READ, mman.MAP_SHARED | mman.MAP_LOCKED,
                                  pageEntry.file.fd,
                                  pageEntry.offset );

        fcntl.posix_fadvise( pageEntry.file.fd,
                             pageEntry.offset,
                             pageEntry.length,
                             fcntl.POSIX_FADV_WILLNEED );

        cached += offset + pageEntry.length;
        
    }

    private void uncache( PageEntry pageEntry ) throws IOException {

        mman.munmap( pageEntry.pa, length );

    }
    
    class FileMeta {

        String path;
        long length;
        int fd;
        
        public FileMeta( String path, long length, int fd ) {
            this.path = path;
            this.length = length;
            this.fd = fd;
        }
        
    }
    
    class PageEntry {

        FileMeta file;
        long offset;
        long length;

        // the pointer of the locked page we will need to unlock later.
        Pointer pa = null;
        
        public PageEntry( FileMeta file, long offset, long length ) {
            this.file = file;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public String toString() {
            return String.format( "%s at offset=%,d and length=%,d" , file.path, offset, length );
        }
        
    }
    
}
