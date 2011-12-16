package peregrine.util.netty;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import peregrine.config.*;
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
public class PrefetchReader implements Closeable {

    private static final Logger log = Logger.getLogger();

    private static final ExecutorService executorService
        = Executors.newCachedThreadPool( new DefaultThreadFactory( PrefetchReader.class ) );

    public static long DEFAULT_PAGE_SIZE = (long)Math.pow( 2, 17 ); 

    /**
     * The minimum number of bytes we should attempt to pre-read at a time.
     */
    public static long DEFAULT_CAPACITY = DEFAULT_PAGE_SIZE * 4;

    public static boolean DEFAULT_ENABLE_LOG = true;
    
    protected long pageSize = DEFAULT_PAGE_SIZE;

    /**
     * The total number of bytes of allocated memory in the cache. 
     */
    protected AtomicLong allocatedMemory = new AtomicLong();

    private boolean closed = false;

    private boolean enableLog = DEFAULT_ENABLE_LOG;

    /**
     * The file metadata for files that still have pending IO.
     */
    private SimpleBlockingQueue<FileMeta> pendingFileQueue = new SimpleBlockingQueue();

    private List<FileMeta> openFiles = new ArrayList();

    /**
     * Pages we have consumed and need to be evicted.
     */
    private SimpleBlockingQueue<PageEntry> consumedPages = new SimpleBlockingQueue();

    private Config config = null;

    private PrefetchReaderListener listener = null;
    
    public PrefetchReader( Config config, List<MappedFile> files ) throws IOException {
        
        this.config = config;
        
        if ( config.getFadviseDontNeedEnabled() == false ) {
            log.warn( "Disabling as fadvise is not enabled." );
            return;
        }

        if ( files.size() == 0 )
            return;
        
        long capacity = config.getSortBufferSize() / files.size();

        log.info( "Running with buffer size: %,d and per file capacity: %,d", config.getSortBufferSize(), capacity );

        for( MappedFile mappedFile : files ) {

            StreamReader reader = mappedFile.getStreamReader();
            
            File file = mappedFile.getFile();

            FileRef fileRef = new FileRef( file.getPath(), file.length(), mappedFile.getFd() );
            
            FileMeta fileMeta = new FileMeta( fileRef, capacity );

            // tell the stream reader that all events need to be handled by this
            // fileMeta.
            reader.setListener( new PrefetchStreamReaderListener( fileMeta ) );

            openFiles.add( fileMeta );
        }

        init();
        
    }

    protected void init() {

        for( FileMeta fileMeta : openFiles ) {
            
            long length = pageSize;

            FileRef fileRef = fileMeta.fileRef;
            
            for( long offset = 0; offset < fileRef.length; offset += pageSize ) {

                if ( offset + length > fileRef.length ) {
                    length = fileRef.length - offset;
                } 

                fileMeta.pendingPages.put( new PageEntry( fileRef, fileMeta, offset, length ) );

                // adjust the length or next time around

                length = pageSize - length;

                if ( length == 0 )
                    length = pageSize;

            }

            pendingFileQueue.put( fileMeta );
            
        }

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

    public void setListener( PrefetchReaderListener listener ) {
        this.listener = listener;
    }
    
    private void fireCacheExhausted() {

        if ( listener == null )
            return;

        listener.onCacheExhausted();

    }

    @Override /* Closeable */
    public void close() throws IOException {

        if ( closed )
            return;
        
        closed = true;

        while( consumedPages.size() > 0 ) {
            evict( consumedPages.take() );
        }

        doEvict();
        
        for( FileMeta file : openFiles ) {

            while( file.cachedPages.size() > 0 ) {
                evict( file.cachedPages.take() );
            }

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
        
        allocatedMemory.addAndGet( pageEntry.length );
        pageEntry.fileMeta.inCache.addAndGet( pageEntry.length );

        pageEntry.fileMeta.cachedHistory.put( pageEntry );
        
    }

    private void evict( PageEntry pageEntry ) throws IOException {

        if ( pageEntry.length == 0 )
            return;

        log( "Evicting %s" , pageEntry );

        mman.munmap( pageEntry.pa, pageEntry.length );

        pageEntry.fileMeta.evictedHistory.put( pageEntry );

        allocatedMemory.addAndGet( -1 * pageEntry.length );
        pageEntry.fileMeta.inCache.addAndGet( -1 * pageEntry.length );

    }

    private void log( String format, Object... args ) {

        if ( enableLog == false )
            return;

        log.info( format, args );
        
    }

    private void doCache() {

        List<FileMeta> files = getFilesForProcessing();

        if ( files.size() == 0 )
            return; /* no pending IO */

        for( FileMeta fileMeta : files ) {

            while( fileMeta.inCache.get() < fileMeta.capacity && fileMeta.pendingPages.size() > 0 ) {

                // never allocate more memory than the sort buffer size.
                if  ( allocatedMemory.get() + pageSize > config.getSortBufferSize() )
                    break;
                
                PageEntry page = fileMeta.pendingPages.take();

                if ( page == null )
                    break;

                try {
                    cache( page );
                } catch ( Throwable t ) {
                    handleThrowable( page, t );
                }

                fileMeta.cachedPages.put( page );

            }

        }

    }

    private void doEvict() {
        doEvict( consumedPages );
    }

    private void doEvict( SimpleBlockingQueue<PageEntry> queue ) {

        while( queue.size() > 0 ) {

            PageEntry page = queue.take();
            
            try {
                evict( page );
            } catch ( Throwable t ) {
                handleThrowable( page, t );
            }

        }

    }
    
    private void handleThrowable( PageEntry page, Throwable t ) {
    
        IOException e = new IOException( "Unable to prefetch: " + page, t );
        
        page.fileMeta.cachedPages.raise( e );
        
    }
    
    private List<FileMeta> getFilesForProcessing() {

        List<FileMeta> result = new ArrayList();

        Iterator<FileMeta> it = pendingFileQueue.iterator();
        
        while( it.hasNext() ) {

            FileMeta fileMeta = it.next();
            
            if ( fileMeta.pendingPages.size() == 0 ) {
                it.remove();
                continue;
            }

            result.add( fileMeta );
            
        }

        Collections.sort( result );

        return result;
        
    }

    class PrefetchStreamReaderListener implements StreamReaderListener {

        private FileMeta fileMeta;
        
        public PrefetchStreamReaderListener( FileMeta fileMeta ) {
            this.fileMeta = fileMeta;
        }
        
        @Override /* StreamReaderListener */
        public void onRead( int length ) {

            fileMeta.readIndex += length;

            try {

                if ( fileMeta.readIndex > fileMeta.cacheIndex ) {

                    if ( fileMeta.current != null ) {
                        consumedPages.put( fileMeta.current );
                    }

                    if ( fileMeta.cachedPages.size() == 0 ) {
                        doEvict();
                        fireCacheExhausted();
                        doCache();
                    }
                    
                    fileMeta.current = fileMeta.cachedPages.take();

                    fileMeta.cacheIndex += fileMeta.readIndex + fileMeta.current.length;

                }

            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }

        }
        
    }

    class FileMeta implements Comparable<FileMeta> {

        protected FileRef fileRef;
        
        /**
         * The readIndex / count of pages read from the stream.
         */
        protected long readIndex = 0;

        /**
         * The amount of data that has been cached.
         *
         * readIndex must always be <= cacheIndex.
         */ 
        protected long cacheIndex = 0;

        /**
         * The number of bytes currently in the cache. 
         */
        protected AtomicLong inCache = new AtomicLong();

        /**
         * The currently cached page.
         */
        protected PageEntry current = null;

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
         * History of pages we have cached for debug purposes.
         */
        protected SimpleBlockingQueue<PageEntry> cachedHistory = new SimpleBlockingQueue();

        protected SimpleBlockingQueue<PageEntry> evictedHistory = new SimpleBlockingQueue();

        protected long capacity = 0;
        
        public FileMeta( FileRef fileRef, long capacity ) {
            this.fileRef = fileRef;
            this.capacity = capacity;
        }

        @Override /* Comparable */
        public int compareTo( FileMeta o ) {
            return (int)inCache.get() - (int)o.inCache.get();
        }
        
    }
    
    class FileRef {

        String path;
        long length;
        int fd;
        
        public FileRef( String path, long length, int fd ) {
            this.path = path;
            this.length = length;
            this.fd = fd;
        }

    }
    
    class PageEntry {

        protected FileRef file  = null;
        protected FileMeta fileMeta = null;
        protected long offset    = 0;
        protected long length    = 0;

        // the pointer of the locked page we will need to unlock later.
        Pointer pa = null;

        public PageEntry() {}
        
        public PageEntry( FileRef file, FileMeta fileMeta, long offset, long length ) {
            this.file = file;
            this.fileMeta = fileMeta;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public String toString() {
            return String.format( "%s at offset=%,d and length=%,d and current capacity %,d" ,
                                  file.path, offset, length, fileMeta.capacity );
        }
        
    }
    
}
