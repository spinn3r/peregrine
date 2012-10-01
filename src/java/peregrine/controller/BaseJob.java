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
package peregrine;

import java.util.*;

import peregrine.config.partitioner.*;
import peregrine.io.*;
import peregrine.rpc.*;
import peregrine.sort.*;
import peregrine.util.*;
import peregrine.controller.*;

import com.spinn3r.log5j.Logger;

/**
 * A 'batch' of jobs sent to the controller at once.
 *
 */
public abstract class BaseJob<T> implements MessageSerializable {

    protected long identifier = -1;

    protected String name = "";

    protected String description = "";

    /**
     * The state of the most recent job that this batch is executing.
     */
    protected String state = JobState.SUBMITTED;

    /**
     * If a job has failed in this batch, this is cause for its failure.
     */
    protected String cause = null;

    protected T instance = null;

    protected long started = 0;
    
    protected long duration = 0;

    protected void init( T instance ) {
        this.instance = instance;
    }

    /**
     * A unique ID for this batch.  This ID is valid for the entire life of the
     * controller and is globally unique.
     */
    public long getIdentifier() {
        return this.identifier;
    }

    public T setIdentifier( long identifier ) {
        this.identifier = identifier;
        return instance;
    }

    /**
     * Get an optionally human readable name for this job.  Should be short and
     * only one line of text.
     */
    public String getName() { 
        return this.name;
    }

    public T setName( String name ) { 
        this.name = name;
        return instance;
    }

    public String getDescription() { 
        return this.description;
    }

    public T setDescription( String description ) { 
        this.description = description;
        return instance;
    }

    /**
     * The given state of this batch from the job that was last executed.
     */
    public String getState() {
        return this.state;
    }

    public T setState( String state ) {
        this.state = state;
        return instance;
    }

    public String getCause() {
        return this.cause;
    }

    public T setCause( String cause ) {
        this.cause = cause;
        return instance;
    }

    public T setCause( Throwable t ) {
        return setCause( Strings.format( t ) );
    }

    /**
     * The time in milliseconds that we were started.
     */
    public long getStarted() {
        return this.started;
    }

    public T setStarted( long started ) {
        this.started = started;
        return instance;
    }

    /**
     * The time in milliseconds that this was run for.
     */
    public long getDuration() {
        return this.duration;
    }

    public T setDuration( long duration ) {
        this.duration = duration;
        return instance;
    }

    @Override
    public Message toMessage() {

        Message message = new Message();

        message.put( "name",          name );
        message.put( "description",   description );
        message.put( "identifier",    identifier );
        message.put( "state",         state );
        message.put( "cause",         cause );
        message.put( "started",       started );
        message.put( "duration",      duration );

        return message;
        
    }

    @Override
    public void fromMessage( Message message ) {

        name          = message.getString( "name" );
        description   = message.getString( "description" );
        identifier    = message.getLong( "identifier" );
        state         = message.getString( "state" );
        cause         = message.getString( "cause" );
        started       = message.getLong( "started" );
        duration      = message.getLong( "duration" );
        
    }

}