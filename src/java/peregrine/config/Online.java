package peregrine.config;

import com.spinn3r.log5j.*;

import peregrine.util.*;

public class Online extends MarkSet<Host>{

	private static final Logger log = Logger.getLogger();

    private Membership membership;
    
    public Online( Membership membership ) {
        this.membership = membership;
    }
    
	@Override
	public void mark( Host entry ) {
		
		if ( ! map.containsKey( entry ) ) 
			log.info( "Host now online: %s" , entry );
            
        if ( membership.getGossip().contains( entry ) ) {
            log.debug( "Host is marked failed by gossip.  Not adding online." );
            return;
        }
        
		put( entry, System.currentTimeMillis() );

	}
	
}
