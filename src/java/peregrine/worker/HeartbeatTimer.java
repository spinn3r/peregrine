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
package peregrine.worker;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;

import peregrine.config.*;
import peregrine.rpc.*;
import peregrine.shuffle.receiver.*;
import peregrine.task.*;
import peregrine.util.*;

import com.spinn3r.log5j.Logger;
import peregrine.util.netty.*;

/**
 * The HeartbeatTimer sends heartbeat messages to the controller regularly.
 */
public class HeartbeatTimer extends Timer {

    private static final Logger log = Logger.getLogger();

    /**
     * Duration to sleep if the controller is online.
     *
     * <p>
     * TODO: make this a config
     */
    public static long CONTROLLER_ONLINE_SLEEP_INTERVAL  = 5000L;

    /**
     * Duration to sleep if the controller is offline.
     * <p>
     * TODO: make this a config
     */
    public static long CONTROLLER_OFFLINE_SLEEP_INTERVAL = 1000L;      

    private boolean cancelled = false;

    private Config config = null;
    
    /**
     *
     */
    public HeartbeatTimer( Config config ) {
        super( HeartbeatTimer.class.getName(), true );

        this.config = config;

        schedule( new HeartbeatTimerTask( this ) , 0 );
        
    }

    @Override
    public void cancel() {
        cancelled = true;
        super.cancel();
    }

    class HeartbeatTimerTask extends TimerTask {

        HeartbeatTimer timer;

        public HeartbeatTimerTask( HeartbeatTimer timer ) {
            this.timer = timer;
        }

        @Override
        public void run() {

            long delay;
            
            if ( config.getMembership().sendHeartbeatToController() ) {
                delay = CONTROLLER_ONLINE_SLEEP_INTERVAL;
            } else {
                delay = CONTROLLER_OFFLINE_SLEEP_INTERVAL;
            }
            
            if ( ! cancelled ) 
                timer.schedule( new HeartbeatTimerTask( timer ), delay );

        }

    }

}
