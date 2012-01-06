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

import peregrine.config.*;
import peregrine.io.driver.*;
import peregrine.io.driver.blackhole.*;
import peregrine.io.driver.shuffle.*;
import peregrine.io.partition.*;
import peregrine.shuffle.sender.*;

/**
 * Factory for obtaining job output from a given Output definition.  
 *
 */
public class JobOutputFactory {

    public static List<JobOutput> getJobOutput( Config config,
                                                Partition partition,
                                                Output output ) throws IOException {

        List<JobOutput> result = new ArrayList( output.getReferences().size() );

        for( OutputReference ref : output.getReferences() ) {

            if ( ref instanceof FileOutputReference ) {

                FileOutputReference fileref = (FileOutputReference)ref;

                PartitionWriter writer = new DefaultPartitionWriter( config, partition, fileref.getPath(), fileref.getAppend() );

                result.add( new PartitionWriterJobOutput( writer ) );

            } else if ( ref instanceof BroadcastOutputReference ) {

                BroadcastOutputReference bcast = (BroadcastOutputReference) ref;
                
                result.add( new BroadcastJobOutput( config, bcast.getName(), partition ) );

            } else {

                IODriver driver = IODriverRegistry.getInstance( ref.getScheme() );
                
                // see if it is registered as a driver.
                if ( driver != null ) {
                    result.add( driver.getJobOutput( ref, config, partition ) );
                    continue;
                }

                throw new IOException( "ref not supported: " + ref.getClass().getName() );

            }

        }

        return result;
        
    }
    
}
