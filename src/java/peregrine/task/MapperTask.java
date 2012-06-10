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

import peregrine.*;
import peregrine.io.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.io.util.*;
import peregrine.map.*;
import peregrine.sysstat.*;
import peregrine.os.*;

import com.spinn3r.log5j.*;

/**
 * Handles executing map jobs.
 */
public class MapperTask extends BaseMapperTask {

    private static final Logger log = Logger.getLogger();

    @Override
    protected void doCall() throws Exception {

        // note a map job with ZERO input files is acceptable.  This would be
        // used for some generator that just emits values on init.
        
        if ( getInput().getReferences().size() != 1 ) {
            throw new Exception( "Map jobs must have exactly one input." );
        }

        jobInput = getJobInput();

        if ( jobInput.size() == 0 )
            return;
        
        SequenceReader reader = jobInput.get( 0 );

        int count = 0;

        try {

            Mapper mapper = (Mapper)jobDelegate;
            
            while( reader.hasNext() ) {

                assertAlive();
                
            	reader.next();
            	
                mapper.map( reader.key(), reader.value() );

                ++count;

            }

        } finally {
            reader.close();
        }
            
        log.info( "Mapped %,d entries on %s on host %s from %s", count, partition, config.getHost(), reader );

    }

}
