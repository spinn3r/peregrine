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
package peregrine.app.flow;

import java.io.*;

import peregrine.*;
import peregrine.controller.*;
import peregrine.io.*;

import com.spinn3r.log5j.Logger;

/**
 * <p>
 * A 'flow' algorithm which starts off at a set of course nodes and finds
 * connectivity between them.
 *
 * <p> The idea is that we start off at source nodes, find their outbound links,
 * then merge them all together , then repeat.
 *
 * <p> One could almost certainly get similar output from pagerank with custom
 * teleporation but this is quick and would be singificantly faster than using
 * custom teleportation if we are able to significantly prune the size of the
 * graph.
 */
public class Flow extends Batch {
    
    private static final Logger log = Logger.getLogger();

    /**
     */
    private String graph = null;

    /**
     * The number of iterations we should perform.
     */
    private int iterations = 5;

    public Flow( String input, String output, String sources, int iterations, boolean caseInsensitive ) {

        this.iterations = iterations;
        
        setName( Flow.class.getName() );

        Job job = new Job();

        map( new Job().setDelegate( FlowInitJob.Map.class )
                      .setInput( input )
                      .setOutput( new Output( "shuffle:default" ) )
                      .setParameters( "sources", sources,
                                      "caseInsensitive", caseInsensitive ) ) ;
        
        reduce( new Job().setDelegate( FlowInitJob.Reduce.class )
                         .setInput( "shuffle:default" )
                         .setOutput( output ) );

        for( int i = 0; i < iterations; ++i ) {

            // now merge against the output and the input
            merge( new Job().setDelegate( FlowIterJob.Merge.class )
                            .setInput( output, input )
                            .setOutput( new Output( "shuffle:default" ) ) );

            reduce( new Job().setDelegate( FlowIterJob.Reduce.class )
                             .setInput( new Input(  "shuffle:default" ) )
                             .setOutput( new Output( output ) ) );
            
        }

    }

}
