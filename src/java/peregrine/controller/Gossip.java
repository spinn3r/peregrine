/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
 * Keeps track of system failures in the controller.  If a majority of machines 
 * have sent gossip that this machine has failed then mark it offline and 
 * no longer schedule work to it or attempt writes.
 * 
 * @author burton
 *
 */
public class Gossip extends MarkMap<Host,MarkSet<Host>> {

	private static final Logger log = Logger.getLogger();

    public static double QUORUM = 0.5;
    
	private Config config;
	
	private Online online;
	
	private Offline offline;
		
	public Gossip( Config config, Online online, Offline offline ) {		
		
		this.config = config;
		this.online = online;
		this.offline = offline;
				
		for( Host host : config.getHosts() ) {
			map.put( host, new MarkSet() );
		}
		
	}

	public void mark( Host reporter, Host failed ) {
		
		// mark the machine as failed... if a majority of host have marked this
		// machine as being failed, take it out of production.
		
		MarkSet<Host> marks = map.get( failed );
		
		marks.mark( reporter );
		
		if ( marks.size() > config.getHosts().size() * QUORUM ) {
			
			log.warn( "Host is no longer online: %s (%,d hosts have gossiped that it has failed.)", failed , marks.size() );
			
			// we can't use this host anymore.  
			offline.mark( failed );

			online.clear( failed );
			
		}
		
	}
    
}
