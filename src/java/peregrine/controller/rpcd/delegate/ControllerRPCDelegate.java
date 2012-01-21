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
package peregrine.controller.rpcd.delegate;

import java.util.*;

import org.jboss.netty.channel.*;

import peregrine.config.Host;
import peregrine.config.Partition;
import peregrine.controller.*;
import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;

/**
 * Delegate for intercepting RPC messages.
 */
public class ControllerRPCDelegate extends RPCDelegate<ControllerDaemon> {

    /**
     * Allows a worker node to report that a partition is complete and its
     * mapper/reducers have executed correctly.
     */
    @RPC
    public void complete( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {
		
        Host host       = Host.parse( message.get( "host" ) );
        Partition part  = new Partition( message.getInt( "partition" ) );

        controllerDaemon.getScheduler().markComplete( host, part );
        
        return;
		
    }
	
    /**
     * Allows a worker node to report that a given partition has failed.  This
     * is usually done if the machine is functioning correctly.  If not we mark
     * the machine failed via other means (such as gossip).
     */
    @RPC
    public void failed( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {
        
        Host host          = Host.parse( message.get( "host" ) );
        Partition part     = new Partition( message.getInt( "partition" ) );
        String stacktrace  = message.get( "stacktrace" );
        boolean killed     = message.getBoolean( "killed" );
        
        controllerDaemon.getScheduler().markFailed( host, part, killed, stacktrace );
	    
        return;
		
    }

    /**
     * Mark a unit of work in a job as complete.  This could be a chunk in a map
     * task, a chunk position list in a merge, or an individual sort in a
     * preemptive reduce sort.
     */
    @RPC
    public void progress( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {

        Host host          = Host.parse( message.get( "host" ) );
        Partition part     = new Partition( message.getInt( "partition" ) );

        return;
		
    }

    /**
     * Allows the controller to receive 'heartbeats' from machines in the
     * cluster so that it can obtain status on the cluster for which machines
     * are 'out there' in the ether.
     */
    @RPC
    public void heartbeat( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {
        
        Host host = Host.parse( message.get( "host" ) );
        
        // verify that the config_checksum is correct...
        if ( ! controllerDaemon.getConfig().getChecksum().equals( message.get( "config_checksum" ) ) ) {
            throw new Exception( "Config checksum from %s is invalid: " + host );
        }
        
        // mark this host as online for the entire controller.
        controllerDaemon.getClusterState().getOnline().mark( host );
		
        return;
		
    }

    /**
     * Allows a worker node to report back that it failed to communicate /
     * collaborate with a given host.  The controller then receives these
     * messages and can make informed decisions about which to mark offline.
     */
    @RPC
    public void gossip( ControllerDaemon controllerDaemon, Channel channel, Message message )
        throws Exception {
        
        // mark that a machine has failed to process some unit of work.
        
        Host reporter = Host.parse( message.get( "reporter" ) );
        Host failed   = Host.parse( message.get( "failed" ) );
        
        controllerDaemon.getClusterState().getGossip().mark( reporter, failed ); 
        
        return;
		
    }
    
}

