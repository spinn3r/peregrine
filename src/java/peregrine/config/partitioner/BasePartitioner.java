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

import peregrine.config.*;

public abstract class BasePartitioner implements Partitioner {

	protected int nr_partitions;

	@Override
    public void init( Config config ) {
    	init( config.getMembership().size() );

    }

    /**
     * Used to make it easier to create directly from various basic
     * configurations.
     */
    public void init( int nr_partitions ) {
        this.nr_partitions = nr_partitions;
    }

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
		
}
