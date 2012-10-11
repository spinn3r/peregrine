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
package peregrine.app.pagerank.extract;

import java.io.*;

import peregrine.*;
import peregrine.controller.*;
import peregrine.io.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class Extract extends Batch {
    
    private static final Logger log = Logger.getLogger();

    public Extract( String path, String graph, String nodes_by_hashcode, boolean caseInsensitive ) {

        super( Extract.class );
        
        map( new Job().setDelegate( CorpusExtractJob.Map.class )
                      .setInput( new Input( "blackhole:" ) )
                      .setOutput( new Output( "shuffle:nodes", "shuffle:links" ) )
                      .setParameters( "path", path,
                                      "caseInsensitive", caseInsensitive ) );
       
        reduce( new Job().setDelegate( UniqueNodeJob.Reduce.class )
                         .setCombiner( UniqueNodeJob.Reduce.class )
                         .setInput( "shuffle:nodes" )
                         .setOutput( nodes_by_hashcode ) );

        reduce( new Job().setDelegate( UniqueOutboundLinksJob.Reduce.class )
                         .setInput( "shuffle:links" )
                         .setOutput( graph ) );

    }

}
