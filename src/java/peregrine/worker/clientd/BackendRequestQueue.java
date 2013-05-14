package peregrine.worker.clientd;

import peregrine.config.Config;
import peregrine.io.sstable.GetBackendRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handle registration of request to the backend into a queue which is then
 * serviced by a dedicated backend thread.  We add keys in lists since clients
 * usually request more than one key and this way we get higher throughput.
 *
 */
public class BackendRequestQueue {

    public static final int LIMIT = 50000; /* FIXME */

    private LinkedBlockingQueue<List<GetBackendRequest>> delegate;

    // keep track of the size of the queue.
    private AtomicInteger size = new AtomicInteger();

    public BackendRequestQueue( Config config ) {
        delegate = new LinkedBlockingQueue();
    }

    public void add( List<GetBackendRequest> list ) {
        delegate.add(list);
        size.getAndAdd( list.size() );
    }

    /**
     * Return true if we have exhausted our internal queue.  This can happen if
     * we can't keep up with the load of requests.  We require that you
     * specify the list of additional keys that you would add to the queue.
     */
    public boolean isExhausted( int keys ) {
       return size.get() + keys >= LIMIT;
    }

    /**
     * Called by anyone wanting to consume threads from this queue and block
     * until we have results.  We attempt to minimize locking here by calling
     * take() once and then calling drainTo for any additional entries.
     *
     * @param target the set of entries in this queue.
     */
    public void drainTo( Collection<GetBackendRequest> target ) {

        try {

            List<GetBackendRequest> first = delegate.take();

            for( GetBackendRequest current : first ) {
                target.add( current );
            }

            Collection<List<GetBackendRequest>> remaining = new ArrayList<List<GetBackendRequest>>();

            delegate.drainTo( remaining );

            for( List<GetBackendRequest> list : remaining ) {

                for( GetBackendRequest current : list ) {
                    target.add(current);
                }

            }

            size.getAndAdd( -target.size() );

        } catch ( InterruptedException e ) {
            throw new RuntimeException(e);
        }

    }

}
