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

package peregrine.worker.clientd;

import peregrine.config.Config;
import peregrine.metrics.WorkerMetrics;
import peregrine.worker.clientd.requests.BackendRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handle registration of request to the backend into a queue which is then
 * serviced by a dedicated backend thread.  We add keys in lists since clients
 * usually request more than one key and this way we get higher throughput.
 *
 */
public class BackendRequestQueue {

    private LinkedBlockingQueue<List<BackendRequest>> delegate;

    // keep track of the size of the queue.
    private AtomicInteger size = new AtomicInteger();

    private Config config;

    private WorkerMetrics workerMetrics;

    public BackendRequestQueue( Config config, WorkerMetrics workerMetrics ) {
        this.config = config;
        this.workerMetrics = workerMetrics;
        delegate = new LinkedBlockingQueue( config.getBackendRequestQueueSize() );
    }

    /**
     * Add the given items to the queue.
     */
    public void add( List<BackendRequest> list ) {
        size.getAndAdd( size( list ) );
        delegate.add(list);

    }

    public int size( BackendRequest sizeable ) {
        return sizeable.size();
    }

        // count the number of keys that this request would involve.
    public int size( Collection<BackendRequest> list ) {

        int result = 0;

        for ( BackendRequest request : list ) {
            result += request.size();
        }

        return result;

    }

    /**
     * Return true if we have exhausted our internal queue.  This can happen if
     * we can't keep up with the load of requests.  We require that you
     * specify the list of additional keys that you would add to the queue.
     */
    public boolean isExhausted( int newRequestsSize ) {
       return size.get() + newRequestsSize >= config.getBackendRequestQueueSize();
    }

    /**
     * Called by anyone wanting to consume threads from this queue and block
     * until we have results.  We attempt to minimize locking here by calling
     * take() once and then calling drainTo for any additional entries.
     *
     * @param target the set of entries in this queue.
     */
    public void drainTo( Collection<BackendRequest> target ) {

        try {

            List<BackendRequest> first = delegate.take();

            for( BackendRequest current : first ) {
                target.add( current );
            }

            Collection<List<BackendRequest>> remaining = new ArrayList<List<BackendRequest>>();

            delegate.drainTo( remaining );

            for( List<BackendRequest> list : remaining ) {

                for( BackendRequest current : list ) {
                    target.add(current);
                }

            }

            size.getAndAdd( -size(target) );

        } catch ( InterruptedException e ) {
            throw new RuntimeException(e);
        }

    }

}
