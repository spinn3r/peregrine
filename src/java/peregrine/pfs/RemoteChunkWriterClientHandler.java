package peregrine.pfs;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.util.*;

/**
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author Andy Taylor (andy.taylor@jboss.org)
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 *
 * @version $Rev: 2189 $, $Date: 2010-02-19 18:02:57 +0900 (Fri, 19 Feb 2010) $
 */
public class RemoteChunkWriterClientHandler extends SimpleChannelUpstreamHandler {

    private HttpRequest request;
    
    public RemoteChunkWriterClientHandler( HttpRequest request ) {
        this.request = request;
    }

    @Override
    public void channelConnected( ChannelHandlerContext ctx, ChannelStateEvent e) {

        e.getChannel().write( request );

    }
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        HttpResponse response = (HttpResponse) e.getMessage();

        System.out.println("STATUS: " + response.getStatus());
        System.out.println("VERSION: " + response.getProtocolVersion());
        System.out.println();

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {

    }

}

