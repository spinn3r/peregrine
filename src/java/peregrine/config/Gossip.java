package peregrine.config;

import com.spinn3r.log5j.*;

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
	
	private Config config;
		
	public Gossip( Config config ) {		
		
		this.config = config;
				
		for( Host host : config.getHosts() ) {
			map.put( host, new MarkSet() );
		}
		
	}

	public void mark( Host reporter, Host failed ) {
		
		// mark the machine as failed... if a majority of host have marked this
		// machine as being failed, take it out of production.
		
		MarkSet marks = map.get( failed );
		
		marks.mark( reporter );
		
		if ( marks.size() > config.getHosts().size() / 2 ) {
			
			log.warn( "Host is no longer online: %s", failed );
			
			// we can't use this host anymore.  
			config.getMembership().getOnline().clear( failed );
			
		}
		
	}
	
}
