package peregrine.util.netty;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.os.*;
import peregrine.util.*;

import com.sun.jna.Pointer;

import com.spinn3r.log5j.Logger;

/**
 * <p>
 * Takes a given set of files and listens to a StreamReader and mlocks pages and
 * then requests that they be read.  Once the pages reads are complete, we evict
 * them from the page cache.
 *
 * <p>
 * The caller can also set the capacity of how much data we need to cache.
 * 
 */
public class PrefetchReader implements StreamReaderListener, Closeable {

    private static final Logger log = Logger.getLogger();

    /* 2^17 is 128k */
    public static long DEFAULT_PAGE_SIZE = (long)Math.pow( 2, 17 ); 

    /**
     * The minimum number of bytes we should attempt to pre-read at a time.
     */
    public static long DEFAULT_CAPACITY = DEFAULT_PAGE_SIZE * 4;

    private static final ExecutorService executorService
        = Executors.newCachedThreadPool( new DefaultThreadFactory( PrefetchReader.class ) );

    /**
     * The readIndex / count of pages read from the stream.
     */
    private long readIndex = 0;

    /**
     * The amount of data that has been cached.
     *
     * readIndex must always be <= cacheIndex.
     */ 
    private long cacheIndex = 0;

    /**
     * The number of bytes currently in the cache. 
     */
    private AtomicLong inCache = new AtomicLong();

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
    protected SimpleBlockingQueue<PageEntry> pendingPages = new SimpleBlockingQueue();

    /**
     * Pages we have consumed and need to be evicted.
     */
    protected SimpleBlockingQueue<PageEntry> consumedPages = new SimpleBlockingQueue();

    protected long pageSize = DEFAULT_PAGE_SIZE;
    
    protected long capacity = DEFAULT_CAPACITY;

    private boolean closed = false;

    private CachingTask task = null;

    private Future taskFuture = null;

    private boolean enableLog = false;

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

            long length = pageSize;

            for( long offset = 0; offset < file.length; offset += pageSize ) {

                if ( offset + length > file.length ) {
                    length = file.length - offset;
                } 

                pendingPages.put( new PageEntry( file, offset, length ) );

                // adjust the length or next time around

                length = pageSize - length;

                if ( length == 0 )
                    length = pageSize;

            }
            
        }

    }

    public void setCapacity( long capacity ) {
        this.capacity = capacity;
    }

    public long getCapacity() {
        return this.capacity;
    }

    public long getPageSize() { 
        return this.pageSize;
    }

    public void setPageSize( long pageSize ) { 
        this.pageSize = pageSize;
    }

    public void setEnableLog( boolean enableLog ) {
        this.enableLog = enableLog;
    }
    
    /**
     * Kick off the prefetcher so that it runs in the background.
     */
    public void start() {

        task = new CachingTask();
        
        taskFuture = executorService.submit( task );
        
    }

    @Override /* Closeable */
    public void close() throws IOException {

        if ( closed )
            return;
        
        closed = true;

        if ( task == null )
            return;
        
        task.shutdown = true;

        // we have to get at least ONE page into consumedPages so that the task
        // doesn't block waiting on pages to be consumed.  We give it an empty
        // reference which causes a noop in evict()
        consumedPages.put( new PageEntry() );

        try {
            taskFuture.get();
        } catch ( Exception e ) {
            throw new IOException( e );
        }

        // now go through ALL pages and evict them.

        if ( current != null )
            evict( current );

        while( consumedPages.size() > 0 ) {
            evict( consumedPages.take() );
        }

        while( cachedPages.size() > 0 ) {
            evict( cachedPages.take() );
        }

    }
    
    /**
     * Cache a page on disk.  
     */
    private void cache( PageEntry pageEntry ) throws IOException {

        log( "Caching %s" , pageEntry );

        pageEntry.pa = mman.mmap( pageEntry.length,
                                  mman.PROT_READ, mman.MAP_SHARED | mman.MAP_LOCKED,
                                  pageEntry.file.fd,
                                  pageEntry.offset );

        fcntl.posix_fadvise( pageEntry.file.fd,
                             pageEntry.offset,
                             pageEntry.length,
                             fcntl.POSIX_FADV_WILLNEED );
        
        inCache.addAndGet( pageEntry.length );

    }

    private void evict( PageEntry pageEntry ) throws IOException {

        if ( pageEntry.length == 0 )
            return;

        log( "Evicting %s" , pageEntry );

        mman.munmap( pageEntry.pa, pageEntry.length );

        inCache.addAndGet( -1 * pageEntry.length );

    }

    @Override /* StreamReaderListener */
    public void onRead( int length ) {

        readIndex += length;

        try {

            if ( readIndex > cacheIndex ) {

                // the current page needs to be evicted on the next call so record
                // it... 
                if ( current != null ) {
                    consumedPages.put( current );
                }
                
                current = cachedPages.take();

                cacheIndex += readIndex + current.length;

            }

        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

    }

    private void log( String format, Object... args ) {

        if ( enableLog == false )
            return;

        log.info( format, args );
        
    }
    
    class CachingTask implements Runnable {

        protected boolean shutdown = false;
        
        public void run() {

            try {

                while( true ) {

                    // if we have pending pages to be read this means that we
                    // have to stick around and read them.
                    if ( pendingPages.size() == 0 )
                        break;

                    // if we have been asked to close externally we need to
                    // yield to this.  Note that this needs to be done AFTER we
                    // have gone through one state of eviction so that mlock
                    // isn't sitting around.
                    if ( shutdown )
                        break;

                    // **** step 1. evict any pages that are consumed.  Note
                    // that we must ALWAYS enter this loop if we have more in
                    // cache than the capacity so that we block on consumedPages
                    // and wait for new IO to complete.  Also always FULL drain
                    // consumedPages if there is anything waiting.

                    while( inCache.get() >= capacity || consumedPages.size() > 0 ) {

                        if ( shutdown )
                            break;

                        PageEntry page = consumedPages.take();

                        try {
                            evict( page );
                        } catch ( IOException e ) {
                            throw new IOException( "Unable to prefetch: " + page, e );
                        }

                    }

                    // **** step 2. cache any pages while we have too few pages
                    // available and we still have pending pages to cache.

                    while( inCache.get() < capacity && pendingPages.size() > 0 ) {

                        if ( shutdown )
                            break;
                        
                        PageEntry page = pendingPages.take();

                        if ( page == null )
                            break;

                        try {
                            cache( page );
                        } catch ( IOException e ) {
                            throw new IOException( "Unable to prefetch: " + page, e );
                        }

                        cachedPages.put( page );

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

        FileMeta file  = null;
        long offset    = 0;
        long length    = 0;

        // the pointer of the locked page we will need to unlock later.
        Pointer pa = null;

        public PageEntry() {}
        
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
