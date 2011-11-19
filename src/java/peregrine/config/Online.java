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

        if ( membership.getOffline().contains( entry ) ) {
            log.debug( "Host is marked offline.  Not adding online." );
            return;
        }

		if ( ! map.containsKey( entry ) ) 
			log.info( "Host now online: %s" , entry );

		put( entry, System.currentTimeMillis() );

	}
	
}
