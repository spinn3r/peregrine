package peregrine.pfs;

import java.io.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.util.*;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;

/**
 */
public class RemoteChunkWriterClientHandler extends SimpleChannelUpstreamHandler {

    private RemoteChunkWriterClientListener listener = null;
    
    public RemoteChunkWriterClientHandler( RemoteChunkWriterClientListener listener ) {
        this.listener = listener;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        HttpResponse response = (HttpResponse) e.getMessage();

        if ( response.getStatus().getCode() != OK.getCode() ) {
            listener.closed = true;
            listener.setCause( new IOException( response.getStatus().toString() ) );
        }

        listener.success();
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {

    }

}

