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
package peregrine.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.config.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.shuffle.sender.*;

/**
 * Represents a way to add new input drivers to peregrine.
 */
public interface InputDriver {

    /**
     * Get the URI scheme for this driver.
     */
    public String getScheme();
    
    public InputReference getInputReference( String uri );

    public JobInput getJobInput( Config config , Partition partition );

}
