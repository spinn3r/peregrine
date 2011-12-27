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
package peregrine.pfsd.rpcd.delegate;

import org.jboss.netty.channel.*;

import peregrine.pfsd.*;
import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;

/**
 */
public class ShufflerRPCDelegate extends RPCDelegate<FSDaemon> {

    public void handleMessage( FSDaemon daemon, Channel channel, Message message )
        throws Exception {
    	
        String action = message.get( "action" );

        if ( "flush".equals( action ) ) {
            // FIXME: this should be async should it not?
            daemon.shuffleReceiverFactory.flush();
            return;

        }

        if ( "purge".equals( action ) ) {
            daemon.shuffleReceiverFactory.purge( message.get( "name" ) );
            return;

        }

        throw new Exception( String.format( "No handler for action %s with message %s", action, message ) );

    }
    
}
