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
import java.util.concurrent.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.controller.*;
import peregrine.io.*;
import peregrine.rpc.*;
import peregrine.util.*;

/**
 *
 * Send the response to a 'status' request as a Message.
 */
public class ControllerStatusResponse implements MessageSerializable {

    protected List<Batch> history = new ArrayList();

    public ControllerStatusResponse() {}

    public ControllerStatusResponse( Controller controller ) {

        //FIXME: include controller scheduler state

        /*
        if ( scheduler != null ) {
            response.put( "scheduler", scheduler.getStatusAsMap() );
        }
        */

        this.history = new ArrayList( controller.getHistory() );
        
    }

    public List<Batch> getHistory() {
        return history;
    }
    
    /**
     * Convert this to an RPC message.
     */
    public Message toMessage() {

        Message response = new Message();

        response.put( "history",   history );

        return response;
        
    }

    public void fromMessage( Message message ) {

        history = new StructList( message.getList( "history" ), Batch.class );
        
    }

}