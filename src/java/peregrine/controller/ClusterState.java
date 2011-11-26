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

