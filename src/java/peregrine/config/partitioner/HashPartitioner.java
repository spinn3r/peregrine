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
package peregrine.config.partitioner;

import peregrine.*;
import peregrine.config.*;
import peregrine.util.*;
import peregrine.util.primitive.*;

/**
 * Partitions by hash code mod the number of partitions.
 */
public class HashPartitioner extends BasePartitioner {
	
	@Override
	public Partition partition( StructReader key ) {

        if ( key.length() < 8 )
            throw new IllegalArgumentException( "key too short: " + key.length() );

		byte[] bytes = key.toByteArray();

        // we only need a FEW bytes to route a key , not the WHOLE thing if it
        // is a hashcode.  For example... we can route to 255 partitions with
        // just one byte... that IS if it is a hashode.  with just TWO bytes we
        // can route to 65536 partitions which is probably fine for all users
        // for a LONG time.

        long value =
            (long)((bytes[7] & 0xFF)     ) +
            (long)((bytes[6] & 0xFF) << 8)
            ;

        value = Math.abs( value );
        
        int partition = (int)value % nr_partitions;
        
        return new Partition( partition );		

	}	

}
