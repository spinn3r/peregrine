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
 * Send the response to a 'status' request as a Message.
 */
public class ControllerStatusResponse implements MessageSerializable {
    
    protected long started = -1;

    protected List<Batch> history = new ArrayList();

    protected List<Batch> pending = new ArrayList();

    protected Batch executing = null;

    protected SchedulerStatusResponse schedulerStatusResponse = null;
    
    public ControllerStatusResponse() {}

    public ControllerStatusResponse( Controller controller, Scheduler scheduler ) {

        this.history = controller.getHistory();
        this.pending = controller.getPending();
        this.executing = controller.getExecuting();
        this.started = controller.getStarted();
        
        if ( scheduler != null ) {
            schedulerStatusResponse = new SchedulerStatusResponse( scheduler );
        }
    }

    public List<Batch> getHistory() {
        return history;
    }

    public List<Batch> getPending() {
        return pending;
    }

    public Batch getExecuting() {
        return executing;
    }

    public long getStarted() { 
        return this.started;
    }

    public void setStarted( long started ) { 
        this.started = started;
    }

    public SchedulerStatusResponse getSchedulerStatusResponse() {
        return schedulerStatusResponse;
    }
    
    /**
     * Convert this to an RPC message.
     */
    public Message toMessage() {

        Message response = new Message();

        response.put( "executing",  executing );
        response.put( "history",    history );
        response.put( "pending",    pending );
        response.put( "scheduler",  schedulerStatusResponse );
        response.put( "started",    started );
        
        return response;
        
    }

    public void fromMessage( Message message ) {

        //TODO: this is ugly but I'm not sure of a way around this. We could use
        //the null object pattern here with an empty Batch but that seems like a
        //bad idea.
        
        if ( message.containsKey( "executing" ) ) {

            executing = new Batch();
            Message msg = new Message( message.getString( "executing" ) );
            executing.fromMessage( msg );
            
        }

        if ( message.containsKey( "scheduler" ) ) {
            schedulerStatusResponse = new SchedulerStatusResponse();
            schedulerStatusResponse.fromMessage( new Message( message.getString( "scheduler" ) ) );
        }

        history    = message.getList( "history", Batch.class );
        pending    = message.getList( "pending", Batch.class );
        started    = message.getLong( "started" );
        
    }

}