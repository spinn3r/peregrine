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
package peregrine.controller;

import com.spinn3r.log5j.*;

import peregrine.config.*;
import peregrine.controller.*;
import peregrine.util.*;

/**
 * Keeps track of hosts once they come online.
 */
public class Online extends MarkMap<Host,Long>{

	private static final Logger log = Logger.getLogger();

    private Offline offline;
    
    public Online( Offline offline ) {
        this.offline = offline;
    }
    
	@Override
	public void mark( Host entry ) {

        if ( offline.contains( entry ) ) {
            log.debug( "Host is marked offline.  Not adding online." );
            return;
        }

		if ( ! map.containsKey( entry ) ) 
			log.info( "Host now online: %s" , entry );

		put( entry, System.currentTimeMillis() );

	}
	
}
