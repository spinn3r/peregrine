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

import com.spinn3r.log5j.Logger;

/**
 * A 'batch' of jobs sent to the controller at once.
 *
 */
public class Batch implements MessageSerializable {

    private static final Logger log = Logger.getLogger();

    protected String description = null;
    
    protected String name = null;

    protected List<Job> jobs = new ArrayList();

    public void add( Job job ) {
        jobs.add( job );
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public String getName() { 
        return this.name;
    }

    public Batch setName( String name ) { 
        this.name = name;
        return this;
    }

    public String getDescription() { 
        return this.description;
    }

    public Batch setDescription( String description ) { 
        this.description = description;
        return this;
    }

    /**
     * Convert this to an RPC message.
     */
    @Override
    public Message toMessage() {

        Message message = new Message();

        message.put( "name",          name );
        message.put( "description",   description );
        message.put( "job",           jobs );

        return message;
        
    }

    @Override
    public void fromMessage( Message message ) {

        name          = message.getString( "name" );
        description   = message.getString( "description" );
        jobs          = new StructList( message.getList( "job" ), Job.class );

    }

    @Override
    public String toString() {
        return jobs.toString();
    }
    
}
