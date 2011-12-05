package peregrine.util.netty;

import java.io.*;
import java.util.*;

import peregrine.os.*;
import peregrine.util.*;

import com.sun.jna.Pointer;

/**
 * Takes a given set of files and listens to a StreamReader and mlocks pages and
 * then requests that they be read.  Once the pages reads are complete, we evict
 * them from the page cache.
 * 
 */
public class PrefetchReader implements StreamReaderListener {

    /* 2^ 17 is 128k */
    public static long PAGE_SIZE = (long)Math.pow( 2, 17 ); 

    /**
     * The minimum number of bytes we should attempt to pre-read at a time.
     */
    public static long CAPACITY = PAGE_SIZE * 4;

    /**
     * The offset / count of pages read from the stream.
     */
    private long offset = 0;

    /**
     * The amount of data that has been cached.  Offset must always be <=
     * cached.
     */ 
    private long cached = 0;

    /**
     * The currently cached page.
     */
    private PageEntry current = null;

    /**
     * Pages that we have cached and are mlocked.
     */
    protected SimpleBlockingQueueWithFailure<PageEntry,IOException> cachedPages
        = new SimpleBlockingQueueWithFailure();
    
    /**
     * Pages pending to be cached.
     */
    protected SimpleBlockingQueue<PageEntry> pendingPages
        = new SimpleBlockingQueue();

    /**
     * Pages we have consumed and need to be evicted.
     */
    protected SimpleBlockingQueue<PageEntry> consumedPages
        = new SimpleBlockingQueue();

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

                pendingPages.put( new PageEntry( file, offset, length ) );

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

        try {

            if ( offset > cached ) {

                // the current page needs to be evicted on the next call so record
                // it... 
                if ( current != null ) {
                    consumedPages.put( current );
                }
                
                current = cachedPages.take();

                cached += offset + current.length;

            }

        } catch ( IOException e ) {
            throw new RuntimeException( e );
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

    }

    private void evict( PageEntry pageEntry ) throws IOException {

        mman.munmap( pageEntry.pa, pageEntry.length );

    }

    class CachingTask implements Runnable {

        public void run() {

            try {

                long inCache = 0;

                while( true ) {

                    // if we have pending pages to be read this means that we
                    // have to stick around and read them.
                    if ( pendingPages.size() == 0 )
                        break;

                    // if these are are consumed then we have to evict them.
                    if ( consumedPages.size() == 0 )
                        break;

                    // this is the tricky case. If they are cached the reader
                    // will eventually add them to consumedPages 
                    if ( cachedPages.size() == 0 )
                        break;

                    // **** step 1. evict any pages that are consumed.  Note
                    // that we must ALWAYS enter this loop if we have more in
                    // cache than the capacity so that we block on consumedPages
                    // and wait for new IO to complete.  Also always FULL drain
                    // consumedPages if there is anything waiting.

                    while( inCache >= CAPACITY || consumedPages.size() > 0 ) {

                        PageEntry page = consumedPages.take();

                        try {
                            evict( page );
                        } catch ( IOException e ) {
                            throw new IOException( "Unable to prefetch: " + page, e );
                        }

                        inCache -= page.length;
                        
                    }

                    // **** step 2. cache any pages while we have too few pages
                    // available.

                    while( inCache < CAPACITY ) {

                        PageEntry page = pendingPages.take();

                        if ( page == null )
                            break;

                        try {
                            cache( page );
                        } catch ( IOException e ) {
                            throw new IOException( "Unable to prefetch: " + page, e );
                        }

                        cachedPages.put( page );
                        
                        inCache += page.length;

                    }

                }

            } catch ( IOException e ) {
                cachedPages.raise( e );
            } catch ( Throwable t ) {
                cachedPages.raise( new IOException( "Unable to prefetch file: ", t ) );
            }

        }
        
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
