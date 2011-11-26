package peregrine.rpc;

import java.io.*;
import java.net.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.config.Host;
import peregrine.http.*;

import com.spinn3r.log5j.*;

/**
 * 
 * 
 */
public class Client {

    private static final Logger log = Logger.getLogger();

    private boolean silent = false;
    
    public Client() {}

    public Client( boolean silent ) {
        this.silent = silent;
    }

    public void invoke( Host host, String service, Message message ) throws IOException {
        invokeAsync( host, service, message ).close();
    }
    
    public HttpClient invokeAsync( Host host, String service, Message message ) throws IOException {

        try {

            String data = message.toString();

            if ( host == null )
                throw new NullPointerException( "host" );
            
            URI uri = new URI( String.format( "http://%s:%s/%s/RPC", host.getName(), host.getPort(), service ) );

            if( ! silent )
                log.info( "Sending RPC %s %s ..." , data, uri );
            
            HttpClient client = new HttpClient( uri );

            client.setMethod( HttpMethod.POST );
            client.write( data.getBytes() );

            return client;
            
        } catch ( URISyntaxException e ) {
            throw new IOException( e );
        }
            
    }

}