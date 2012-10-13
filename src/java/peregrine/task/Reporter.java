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

import java.util.*;

import peregrine.config.partitioner.*;
import peregrine.io.*;
import peregrine.rpc.*;
import peregrine.sort.*;
import peregrine.util.*;
import peregrine.controller.*;

import com.spinn3r.log5j.Logger;

/**
 * A Reporter allows a job to report progress to the controller for tracking and
 * auditing purposes.
 */
public class Reporter {

    // the number of consumed records. This should be ever increasing.  A job
    // MAY NOT actually be emitting anything but it DOES need to consume
    // records.
    private Metric consumed = new Metric();
    
    // the number of emitted records.  This should be ever increasing.
    private Metric emitted = new Metric();

    // the progress of our job between 0 and 100.
    private Metric progress = new Metric( 100 );

    public Metric getConsumed() {
        return consumed;
    }
    
    public Metric getEmitted() {
        return emitted;
    }

    public Metric getProgress() {
        return progress;
    }
    
    public class Metric {

        private long value;

        private long max = Long.MAX_VALUE;

        public Metric() {}

        public Metric( long max ) {
            this.max = max;
        }

        public void incr() {
            ++value;
        }

        public long get() {
            return value;
        }

        @Override
        public String toString() {
            return "" + value;
        }
        
    }
    
}
