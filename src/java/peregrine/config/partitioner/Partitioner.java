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

/**
 * Interface for handling partitioning keys.  Takes a given key and routes it to
 * the correct partition.
 */
public interface Partitioner {

	/**
	 * Init the range router with the given config so that we can
     * 
	 * @param config The config to read configuration data.
	 */
	public void init( Config config );

	/**
	 * Init the partitioner with the current job.
     * 
	 * @param config The config to read configuration data.
	 */
	public void init( Job job );

    /**
     * Init the partitioner with the local context of the job which we are about
     * to execute.
     */
    public void init( LocalContext LocalContext );

    /**
     * Route the given key to a given partition.
     */
	public Partition partition( StructReader key, StructReader value );
	
}
