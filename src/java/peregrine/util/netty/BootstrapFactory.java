package peregrine.util.netty;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.socket.nio.*;

public class BootstrapFactory {

    public static boolean TCP_NODELAY = true;

    public static long CONNECT_TIMEOUT_MILLIS = 5000;
	
	public static ServerBootstrap newServerBootstrap( NioServerSocketChannelFactory factory ) {
		
		ServerBootstrap bootstrap = new ServerBootstrap( factory );

		setOptions( bootstrap );
		setOptions( "child.", bootstrap );

        return bootstrap;
        
	}
	
	public static ClientBootstrap newClientBootstrap( NioClientSocketChannelFactory factory ) {
	
		// Configure the client.	
		ClientBootstrap bootstrap = new ClientBootstrap( factory );
		
		setOptions( bootstrap );
		
		return bootstrap;
		
	}

	private static void setOptions( Bootstrap bootstrap ) {
		setOptions( "", bootstrap );
	}
		
	private static void setOptions( String prefix, Bootstrap bootstrap ) {
		
        // set options 	
        bootstrap.setOption( prefix+"tcpNoDelay",            TCP_NODELAY );
        bootstrap.setOption( prefix+"connectTimeoutMillis",  CONNECT_TIMEOUT_MILLIS );
        bootstrap.setOption( prefix+"bufferFactory",         new DirectChannelBufferFactory() );
		
	}
	
}
