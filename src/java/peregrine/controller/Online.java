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
