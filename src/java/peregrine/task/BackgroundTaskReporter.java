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
 * Background runnable or reporting task progress via Reporter.
 */
public class BackgroundTaskReporter implements Callable<Boolean> {

    public static final long SLEEP_INTERVAL = 1000L;

    private BaseTask task;
    
    public BackgroundTaskReporter( BaseTask task ) {
        this.task = task;
    }
    
    public Boolean call() throws Exception {

        while( true ) {

            //TODO: this must be sent BEFORE we check the task status because
            //that way if we are 100% complete we send the last progress
            
            task.sendProgressToController();
            
            if ( task.status != TaskStatus.UNKNOWN )
                return true;

            // send the current state of the Reporter to the controller. 
            
            Thread.sleep( SLEEP_INTERVAL );

        }
        
    }

}
