package peregrine.config;

import com.spinn3r.log5j.*;

import peregrine.util.*;

public class Online extends ConcurrentSet<Host>{

	private static final Logger log = Logger.getLogger();
	
	public void mark( Host entry ) {
		
		if ( ! map.containsKey( entry ) )
			log.info( "Host now online: %s" , entry );
		
		map.put( entry, System.currentTimeMillis() );
		
	}
	
}
