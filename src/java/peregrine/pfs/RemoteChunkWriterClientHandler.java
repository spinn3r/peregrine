package peregrine.pfs;

import java.io.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class RemoteChunkWriterClientHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    private RemoteChunkWriterClient client = null;
    
    public RemoteChunkWriterClientHandler( RemoteChunkWriterClient client ) {
        this.client = client;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        Object message = e.getMessage();
        
        if ( message instanceof HttpResponse ) {
        
            HttpResponse response = (HttpResponse) e.getMessage();
            
            log.info( "Received HTTP response: %s for %s", response.getStatus(), client.uri );

            client.channelState = RemoteChunkWriterClient.CLOSED;
            
            if ( response.getStatus().getCode() != OK.getCode() ) {
                client.failed( new IOException( response.getStatus().toString() ) );
                return;
            }
            
            client.success();

        }
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {

        client.failed( e.getCause() );

    }

}

