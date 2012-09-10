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
package peregrine.map;

import java.io.*;
import java.util.*;

import peregrine.*;
import peregrine.io.*;
import peregrine.io.driver.broadcast.*;
import peregrine.shuffle.sender.*;
import peregrine.task.*;

public abstract class BaseMapper extends BaseJobDelegate {

    @Override
    public void init( Job job, List<JobOutput> output ) {

        super.init( job, output );

        //if ( this.stdout instanceof BroadcastJobOutput ) {
        //    throw new RuntimeException( "Standard output may not be a broadcast reference: " + this.stdout );
        //}
        
    }

    /**
     * Used so that map jobs can output periodic rollup / aggregate function
     * data per every chunk to a broadcast file.  For example, with pagerank we
     * can use this to compute the global rank sum so that every time we process
     * an item we add to an accumulator.  Then we emit the value of the
     * accumulator, and reset on every chunk end.
     */
    public void onChunkEnd() {

    }

}

