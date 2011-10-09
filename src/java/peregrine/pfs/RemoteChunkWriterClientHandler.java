package peregrine.pfs;

import java.io.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.util.*;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;

import com.spinn3r.log5j.Logger;

/**
 */
public class RemoteChunkWriterClientHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    private RemoteChunkWriterClientListener listener = null;
    
    public RemoteChunkWriterClientHandler( RemoteChunkWriterClientListener listener ) {
        this.listener = listener;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        HttpResponse response = (HttpResponse) e.getMessage();

        listener.closed = true;

        log.info( "Received HTTP response: %s for %s", response.getStatus(), listener.uri );
        
        if ( response.getStatus().getCode() != OK.getCode() ) {
            listener.setCause( new IOException( response.getStatus().toString() ) );
        }

        listener.success();
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {

    }

}

