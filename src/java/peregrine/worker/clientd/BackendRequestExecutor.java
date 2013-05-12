package peregrine.worker.clientd;

import peregrine.config.Config;
import peregrine.io.sstable.GetBackendRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * Listens to the queue of items submitted by clients and executes them in a
 * background thread.
 *
 */
public class BackendRequestExecutor implements Runnable {

    private Config config;

    private BackendRequestQueue queue;

    public BackendRequestExecutor(Config config, BackendRequestQueue queue) {
        this.config = config;
        this.queue = queue;
    }

    @Override
    public void run() {

        while( true ) {

            List<GetBackendRequest> list = new ArrayList();

            queue.drainTo(list);

        }

    }
}
