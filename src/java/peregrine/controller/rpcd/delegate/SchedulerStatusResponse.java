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
import peregrine.task.*;
import peregrine.util.*;

/**
 *
 * Scheduler status.
 */
public class SchedulerStatusResponse implements MessageSerializable {
    
    private int progress;
    
    public SchedulerStatusResponse() {}

    public SchedulerStatusResponse( Scheduler scheduler ) {
        this.progress = scheduler.getProgress();
    }

    public int getProgress() {
        return progress;
    }

    /**
     * Convert this to an RPC message.
     */
    public Message toMessage() {

        Message response = new Message();
        response.put( "progress", progress );
        
        return response;
        
    }

    public void fromMessage( Message message ) {
        this.progress = message.getInt( "progress" );
            
    }

}