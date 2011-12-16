package peregrine.controller;

import com.spinn3r.log5j.*;

import peregrine.config.*;
import peregrine.util.*;

/**
 * Keeps track off hosts that were explicitly marked offline.  
 */
public class Offline extends MarkSet<Host>{

	private static final Logger log = Logger.getLogger();

	@Override
	public void mark( Host entry ) {

		if ( ! contains( entry ) ) 
			log.info( "Host now offline: %s" , entry );

        super.mark( entry );
        
	}

}
