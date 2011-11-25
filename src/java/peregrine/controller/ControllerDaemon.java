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

public class ControllerDaemon extends BaseDaemon {

    private static final Logger log = Logger.getLogger();

    private Scheduler scheduler = null;

    private Controller controller = null;
    
    public ControllerDaemon( Controller controller, Config config ) {

        this.controller = controller;
        this.setConfig( config );

        init();
        
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
    
}
