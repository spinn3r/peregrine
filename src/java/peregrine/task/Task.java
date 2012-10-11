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
package peregrine.task;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.*;
import peregrine.io.driver.shuffle.*;
import peregrine.rpc.*;
import peregrine.shuffle.sender.*;
import peregrine.sysstat.*;
import peregrine.task.*;

import com.spinn3r.log5j.*;

/**
 * Task interface.
 */
public interface Task extends Callable {

    /**
     * Mark this task as killed.
     */
    public void setKilled( boolean killed );

    public boolean isKilled();
    
    /**
     * Assert that the controller has an active job and have not been marked
     * killed by the controller due to failure.
     */
    public void assertActiveJob() throws IOException;

    public void setInput( Input input );
    
    public void setOutput( Output output );

    public void init( Config config, Work work, Class delegate ) throws IOException;

    public void setJob( Job job );

    public Job getJob();
    
}
