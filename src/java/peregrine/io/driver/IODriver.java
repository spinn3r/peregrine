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

import peregrine.config.*;
import peregrine.io.*;
import peregrine.task.*;

/**
 * Represents a way to add new input drivers to peregrine.
 */
public interface IODriver {

    /**
     * Get the URI scheme for this driver.  For example 'http' , 'file', 
     * 'mysql', 'cassandra', etc.
     */
    public String getScheme();
    
    /**
     * Parse the given URI and return an InputReference we can use.
     */
    public InputReference getInputReference( String uri );

    /**
     * Get a unit of work (input split, partition, etc) from the given string specification.
     */
    public Work getWork( String work );
    
    public JobInput getJobInput( InputReference inputReference , Config config, Work work ) throws IOException;

    public OutputReference getOutputReference( String uri );

    public JobOutput getJobOutput( OutputReference outputReference, Config config, Work Work ) throws IOException;
    
}
