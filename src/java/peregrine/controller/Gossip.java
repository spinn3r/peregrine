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
