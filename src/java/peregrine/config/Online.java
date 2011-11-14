package peregrine.config;

import com.spinn3r.log5j.*;

import peregrine.util.*;

public class Online extends MarkSet<Host>{

	private static final Logger log = Logger.getLogger();
	
	@Override
	public void mark( Host entry ) {
		
		if ( ! map.containsKey( entry ) )
			log.info( "Host now online: %s" , entry );
		
		put( entry, System.currentTimeMillis() );

	}
	
}
