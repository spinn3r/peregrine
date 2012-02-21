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
package peregrine.io.driver;

import java.io.*;
import java.util.*;

import peregrine.config.*;
import peregrine.io.*;
import peregrine.task.*;

/**
 * Supports adding new input drivers to peregrine.
 */
public interface IODriver {

    /**
     * Get the URI scheme for this driver.  For example 'http' , 'file', 
     * 'mysql', 'cassandra', etc.
     */
    public String getScheme();
    
    /**
     * Parse the given URI and return an {@link InputReference} we can use.
     */
    public InputReference getInputReference( String uri );

    /**
     * Get work (input splits, partitions, etc) from the given {@link InputReference}. 
     *
     * This is used by the {@link Scheduler} to determine what needs to be executed.
     */
    public Map<Host,List<Work>> getWork( Config config,
                                         InputReference inputReference ) throws IOException;

    /** 
     * Get a {@link Work} class parsed out from the given URI.
     */
    public WorkReference getWorkReference( String uri );
    
    /**
     * Given a given unit of {@link Work}, and an {@link InputReference} ,
     * return a {@link JobInput} for reading key / value pairs from a job.  The 
     * given {@link WorkReference} is provided so that we can parse out any work 
     * specific data for executing this taks.
     */
    public JobInput getJobInput( Config config,
                                 InputReference inputReference ,
                                 WorkReference work ) throws IOException;

    /**
     * Get an {@link OutputReference} from the given URI.
     */
    public OutputReference getOutputReference( String uri );

    /**
     * For a given {@link WorkReference}, produce a {@link JobOutput} for
     * writing the output of the job.
     */
    public JobOutput getJobOutput( Config config,
                                   OutputReference outputReference,
                                   WorkReference Work ) throws IOException;
    
}
