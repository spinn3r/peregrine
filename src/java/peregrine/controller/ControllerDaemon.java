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
package peregrine.controller;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.logging.*;
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;

import peregrine.config.*;
import peregrine.shuffle.receiver.*;
import peregrine.task.*;
import peregrine.util.*;
import com.spinn3r.log5j.Logger;
import peregrine.util.netty.*;
import peregrine.*;

/**
 * Basic daemon for running the controller only (no FS operations).
 */
public class ControllerDaemon extends BaseDaemon {

    private static final Logger log = Logger.getLogger();

    private Scheduler scheduler = null;

    private Controller controller = null;

    private ClusterState clusterState;

    public ControllerDaemon( Controller controller, 
    		                 Config config,
                             ClusterState clusterState ) {

        this.controller = controller;
        this.setConfig( config );
        this.clusterState = clusterState;
        
        init();
        
        clusterState.init( this );
        
    }
    
    @Override
    public void shutdown() {
    	clusterState.shutdown();
    	super.shutdown();
    }

    @Override
    public ChannelPipelineFactory getChannelPipelineFactory() {
        return new ControllerPipelineFactory( this, getConfig() );
    }
    
    public Scheduler getScheduler() { 
        return this.scheduler;
    }

    public void setScheduler( Scheduler scheduler ) { 
        this.scheduler = scheduler;
    }

    public Controller getController() {
        return controller;
    }

    public ClusterState getClusterState() { 
        return this.clusterState;
    }

}
