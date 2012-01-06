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
package peregrine.shuffle.sender;

import peregrine.*;
import peregrine.config.*;
import peregrine.io.driver.shuffle.*;

public class BroadcastJobOutput extends ShuffleJobOutput {

    public BroadcastJobOutput( Config config, Partition partition ) {
        this( config, "default", partition );
    }
        
    public BroadcastJobOutput( Config config, String name, Partition partition ) {
        super( config, name, partition );
    }
    
    @Override
    public void emit( StructReader key , StructReader value ) {

        Membership membership = config.getMembership();

        for ( Partition target : membership.getPartitions() ) {
            emit( target.getId() , key, value );
            key.reset();
            value.reset();
        }

    }

    @Override
    public String toString() {
        return String.format( "%s:%s@%s", getClass().getName(), name, Integer.toHexString(hashCode()) );
    }
    
}

