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
package peregrine.rpcd.delegate;

import java.lang.reflect.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;

import peregrine.rpc.*;

/**
 * Handle messages we need to invoke.
 */
public abstract class RPCDelegate<T> {

    /**
     * @param parent The parent object invoking this request. Usually an
     * FSDaemon or a Controller.
     */
    public ChannelBuffer handleMessage( T parent,
                                        Channel channel,
                                        Message message )
        throws Exception {

        String action = message.get( "action" );

        Method method = getClass().getMethod( action, new Class[] { parent.getClass(),
                                                                    Channel.class,
                                                                    Message.class } );
        
        return (ChannelBuffer)method.invoke( this, parent, channel, message );
        
    }

}
