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
package peregrine.worker;

import org.jboss.netty.channel.*;

import peregrine.config.*;
import peregrine.shuffle.receiver.*;
import peregrine.util.netty.*;

import com.spinn3r.log5j.Logger;
import peregrine.worker.clientd.BackendRequestExecutor;
import peregrine.worker.clientd.BackendRequestQueue;

/**
 * Main daemon for handling filesystem operations.
 */
public class WorkerDaemon extends BaseDaemon {

    private static final Logger log = Logger.getLogger();

    private ShuffleReceiverFactory shuffleReceiverFactory;

    private HeartbeatTimer heartbeatTimer;

    // general queue used for handling client requests.
    private BackendRequestQueue backendRequestQueue;

    public WorkerDaemon(Config config) {

        setConfig( config );

        init();

        shuffleReceiverFactory = new ShuffleReceiverFactory( config ); 

        if ( config.getController() != null )
            heartbeatTimer = new HeartbeatTimer( config );

        // create a queue that we are going to use for the executor.
        backendRequestQueue = new BackendRequestQueue( config );

        // kick off the request executor thread so that we can start processing results.
        newDefaultThreadPool(BackendRequestExecutor.class)
                .submit(new BackendRequestExecutor(config, backendRequestQueue));

    }

    public BackendRequestQueue getBackendRequestQueue() {
        return backendRequestQueue;
    }

    /**
     * Each daemon can only have one shuffle instance.
     */
    public ShuffleReceiverFactory getShuffleReceiverFactory() {
        return shuffleReceiverFactory;
    }

    @Override
    public ChannelPipelineFactory getChannelPipelineFactory() {
        return new WorkerPipelineFactory( getConfig(), this );
    }
    
    public void shutdown() {

        if ( heartbeatTimer != null )
            heartbeatTimer.cancel();

        super.shutdown();
        
    }

}
