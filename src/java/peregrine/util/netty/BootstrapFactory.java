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
package peregrine.util.netty;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.socket.nio.*;

import com.spinn3r.log5j.*;

public class BootstrapFactory {

	private static final Logger log = Logger.getLogger();
	
    public static boolean TCP_NODELAY = true;

    public static long CONNECT_TIMEOUT_MILLIS = 5000;
	
    public static boolean logged = false;
    
	public static ServerBootstrap newServerBootstrap( NioServerSocketChannelFactory factory ) {
		
		ServerBootstrap bootstrap = new ServerBootstrap( factory );

		setOptions( bootstrap );
		setOptions( "child.", bootstrap );
		logged = true;
		
        return bootstrap;
        
	}
	
	public static ClientBootstrap newClientBootstrap( NioClientSocketChannelFactory factory ) {
	
		// Configure the client.	
		ClientBootstrap bootstrap = new ClientBootstrap( factory );
		
		setOptions( bootstrap );
		logged = true;

		return bootstrap;
		
	}

	private static void setOptions( Bootstrap bootstrap ) {
		setOptions( "", bootstrap );
	}
		
	private static void setOptions( String prefix, Bootstrap bootstrap ) {
		
        // set options 	
        setOption( bootstrap, prefix+"tcpNoDelay",            TCP_NODELAY );
        setOption( bootstrap, prefix+"connectTimeoutMillis",  CONNECT_TIMEOUT_MILLIS );
        setOption( bootstrap, prefix+"bufferFactory",         new DirectChannelBufferFactory() );
		
	}
	
	private static void setOption( Bootstrap bootstrap, String name, Object value ) {
		
		if ( ! logged )
			log.info( "Setting bootstrap: %s=%s", name, value );
		
		bootstrap.setOption( name, value );
		
	}
	
}
