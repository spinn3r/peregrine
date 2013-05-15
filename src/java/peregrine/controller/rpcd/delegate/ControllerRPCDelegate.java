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
package peregrine.controller.rpcd.delegate;

import java.util.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;
import peregrine.task.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;

import com.spinn3r.log5j.*;

/**
 * Delegate for intercepting RPC messages.
 */
public class ControllerRPCDelegate extends RPCDelegate<ControllerDaemon> {

    private static final Logger log = Logger.getLogger();
    
    /**
     * Allows a worker node to report that a partition is complete and its
     * mapper/reducers have executed correctly.
     */
    @RPC
    public ChannelBuffer complete( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {

        Host host     = Host.parse( message.get( "host" ) );
        Input input   = new Input( message.getList( "input" ) );
        Work work     = new Work( host, input, message.getList( "work" ) );

        Scheduler scheduler = controllerDaemon.getScheduler();

        if ( scheduler != null )
            scheduler.markComplete( host, work );
        
        return null;
		
    }
	
    /**
     * Allows a worker node to report that a given partition has failed.  This
     * is usually done if the machine is functioning correctly.  If not we mark
     * the machine failed via other means (such as gossip).
     */
    @RPC
    public ChannelBuffer failed( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {
        
        Host host          = Host.parse( message.get( "host" ) );
        Input input        = new Input( message.getList( "input" ) );
        Work work          = new Work( host, input, message.getList( "work" ) );
        String stacktrace  = message.get( "stacktrace" );
        boolean killed     = message.getBoolean( "killed" );
        
        Scheduler scheduler = controllerDaemon.getScheduler();

        if ( scheduler != null )
            scheduler.markFailed( host, work, killed, stacktrace );
	    
        return null;
		
    }

    /**
     * Allows a worker to tell a controller that a given chunk within a unit of
     * work has been executed.  This could be a chunk in a map task, a chunk
     * position list in a merge, or an individual sort in a preemptive reduce
     * sort or even the final reduce sort.
     */
    @RPC
    public ChannelBuffer progress( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {
        
        Report report = new Report();
        report.fromMessage( message.getMessage( "report" ) );

        Scheduler scheduler = controllerDaemon.getScheduler();

        if ( scheduler != null )
            scheduler.updateReport( report );

        return null;
		
    }

    /**
     * Allows the controller to receive 'heartbeats' from machines in the
     * cluster so that it can obtain status on the cluster for which machines
     * are 'out there' in the ether.
     */
    @RPC
    public ChannelBuffer heartbeat( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {
        
        Host host = Host.parse( message.get( "host" ) );
        
        // verify that the config_checksum is correct...
        if ( ! controllerDaemon.getConfig().getChecksum().equals( message.get( "config_checksum" ) ) ) {
            throw new Exception( "Config checksum from %s is invalid: " + host );
        }
        
        // mark this host as online for the entire controller.
        controllerDaemon.getClusterState().getOnline().mark( host );
		
        return null;
		
    }

    /**
     * Allows a worker node to report back that it failed to communicate /
     * collaborate with a given host.  The controller then receives these
     * messages and can make informed decisions about which to mark offline.
     */
    @RPC
    public ChannelBuffer gossip( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {
        
        // mark that a machine has failed to process some unit of work.
        
        Host reporter = Host.parse( message.get( "reporter" ) );
        Host failed   = Host.parse( message.get( "failed" ) );
        
        controllerDaemon.getClusterState().getGossip().mark( reporter, failed ); 
        
        return null;
		
    }

    /**
     * Get the current controller status as a message/map from the controller
     * including scheduler information.
     */
    @RPC
    public ChannelBuffer status( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {

        ControllerStatusResponse response = new ControllerStatusResponse( controllerDaemon.getController(),
                                                                          controllerDaemon.getScheduler() );

        return response.toMessage().toChannelBuffer();

    }

    /**
     * Take a batch message and hand it to the controller.
     */
    @RPC
    public ChannelBuffer submit( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {
        
        Batch batch = new Batch();
        batch.fromMessage( new Message( message.get( "batch" ) ) );

        // submit the batch for execution
        batch = controllerDaemon.getController().submit( batch );

        // the batch will now have an identifier and we should return it as part of the response

        Message result = new Message();
        result.put( "identifier", batch.getIdentifier() );
        
        return result.toChannelBuffer();
		
    }

    /**
     * Wait for a given batch job to complete and return successfuly once we
     * have completed.
     */
    @RPC
    public ChannelBuffer waitFor( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {

        List<Batch> history = controllerDaemon.getController().getHistory();

        long identifier = message.getLong( "identifier" );
        
        if ( history.size() == 0 || history.get( 0 ).getIdentifier() > identifier ) {
            throw new Exception( "Batch job not yet complete: " + identifier );
        }

        // try the batch and return it.
        
        return null;

    }
    
}

