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

import peregrine.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.io.util.*;
import peregrine.map.*;
import peregrine.merge.*;
import peregrine.os.*;
import peregrine.sysstat.*;

import com.spinn3r.log5j.*;

public class MergerTask extends BaseMapperTask {

    private static final Logger log = Logger.getLogger();

    protected void doCall() throws Exception {

        log.info( "Running merge jobs on host: %s ...", host );

        jobInput = getJobInput();

        MergeRunner mergeRunner = new MergeRunner( jobInput );

        Merger merger = (Merger)jobDelegate;
        Closer closer = new Closer( mergeRunner );
        
        try { 

            while( true ) {

                MergedValue joined = mergeRunner.next();

                if ( joined == null )
                    break;

                assertActiveJob();
                
                merger.merge( joined.key, joined.values );

                reporter.getConsumed().incr();

            }

            log.info( "Running merge jobs on host: %s ... done", host );

        } finally {
            closer.close();
        }
            
    }
    
}

