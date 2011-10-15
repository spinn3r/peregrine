package peregrine.pfs;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.io.async.*;
import peregrine.pfsd.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 * 
 * 
 */
public class RPC {

    private static final Logger log = Logger.getLogger();

    public void invoke( Host host, String service, Map<String,String> message ) throws IOException {

        try {
        
            QueryStringEncoder encoder = new QueryStringEncoder( "" );

            for( String key : message.keySet() ) {
                encoder.addParam( key, message.get( key ) );
            }

            String data = encoder.toString();

            URI uri = new URI( String.format( "http://%s:%s/%s/RPC", host.getName(), host.getPort(), service ) );

            log.info( "Sending RPC %s %s ..." , data, uri );
            
            RemoteChunkWriterClient client = new RemoteChunkWriterClient( uri );

            client.setMethod( HttpMethod.POST );
            client.write( data.getBytes() );
            client.close();

        } catch ( URISyntaxException e ) {
            throw new IOException( e );
        }
            
    }

}