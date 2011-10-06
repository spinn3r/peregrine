package peregrine.pfs;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.CharsetUtil;
import org.jboss.netty.channel.ExceptionEvent;

/**
 * @author <a href="http://www.jboss.org/netty/">The Netty Project</a>
 * @author Andy Taylor (andy.taylor@jboss.org)
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 *
 * @version $Rev: 2189 $, $Date: 2010-02-19 18:02:57 +0900 (Fri, 19 Feb 2010) $
 */
public class HttpResponseHandler extends SimpleChannelUpstreamHandler {

    private boolean readingChunks;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        System.out.printf( "FIXME: GOT RESPONSE\n" );

        if (!readingChunks) {
            HttpResponse response = (HttpResponse) e.getMessage();

            System.out.println("STATUS: " + response.getStatus());
            System.out.println("VERSION: " + response.getProtocolVersion());
            System.out.println();

            if (!response.getHeaderNames().isEmpty()) {
                for (String name: response.getHeaderNames()) {
                    for (String value: response.getHeaders(name)) {
                        System.out.println("HEADER: " + name + " = " + value);
                    }
                }
                System.out.println();
            }

            if (response.isChunked()) {
                readingChunks = true;
                System.out.println("CHUNKED CONTENT {");
            } else {
                ChannelBuffer content = response.getContent();
                if (content.readable()) {
                    System.out.println("CONTENT {");
                    System.out.println(content.toString(CharsetUtil.UTF_8));
                    System.out.println("} END OF CONTENT");
                }
            }
        } else {
            HttpChunk chunk = (HttpChunk) e.getMessage();
            if (chunk.isLast()) {
                readingChunks = false;
                System.out.println("} END OF CHUNKED CONTENT");
            } else {
                System.out.print(chunk.getContent().toString(CharsetUtil.UTF_8));
                System.out.flush();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {

        Channel ch = e.getChannel();
        Throwable cause = e.getCause();
        
        //System.out.printf( "FIXME exception caught: \n" );
        cause.printStackTrace();
        
    }

}

