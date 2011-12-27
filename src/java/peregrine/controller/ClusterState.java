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
import peregrine.util.netty.*;
import peregrine.*;

import com.spinn3r.log5j.Logger;

/**
 * Keeps track of the state of the whole cluster.  Holds references go gossip,
 * offline and online status, the config, controller, etc.
 */
public class ClusterState {
    
    private Gossip gossip = null;
    
    private Offline offline = null;
    
    private Online online = null;

    private Config config;

    private Controller controller;
    
    private MarkOfflineTimer markOfflineTimer = null;
    
    public ClusterState( Config config , Controller controller ) {
        this.config = config;
        this.controller = controller;

        this.offline = new Offline();
        
        this.online = new Online( offline );

        this.gossip = new Gossip( config, online, offline );

    }
    
    public Online getOnline() { 
        return this.online;
    }

    public void setOnline( Online online ) { 
        this.online = online;
    }

    public void setOffline( Offline offline ) { 
        this.offline = offline;
    }

    public Offline getOffline() { 
        return this.offline;
    }

    public Gossip getGossip() { 
        return this.gossip;
    }

    public void setGossip( Gossip gossip ) { 
        this.gossip = gossip;
    }

    public void init( ControllerDaemon daemon ) {
    	markOfflineTimer = new MarkOfflineTimer( config, this );
    }
    
    public void shutdown() {
    	markOfflineTimer.cancel();
    }
    
}

