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
package peregrine.worker.rpcd.delegate;

import java.io.*;

import org.jboss.netty.channel.*;

import peregrine.rpc.*;
import peregrine.rpcd.delegate.*;
import peregrine.worker.*;

import peregrine.os.*;

/**
 * Handles all shuffle related RPC messages.
 */
public class SystemRPCDelegate extends RPCDelegate<FSDaemon> {

    /**
     * Terminate the current daemon.  This is similar to kill and sends the SIGKILL
     * signal AKA 'kill pid'
     */
    @RPC
    public void term( FSDaemon daemon, Channel channel, Message message ) throws Exception {
        signal.kill( unistd.getpid(), signal.SIGTERM );
    }

    /**
     * Kill the current daemon.  This is similar to kill -9 and sends the
     * SIGKILL signal AKA 'kill -9 pid'
     */
    @RPC
    public void kill( FSDaemon daemon, Channel channel, Message message ) throws Exception {
        signal.kill( unistd.getpid(), signal.SIGKILL );
    }

}
