package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelFutureProgressListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.DefaultFileRegion;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.FileRegion;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedFile;
import org.jboss.netty.util.CharsetUtil;

import peregrine.Config;
import peregrine.io.async.*;
import peregrine.util.*;

/**
 */
public class FilesystemHandler extends SimpleChannelUpstreamHandler {

    private HttpRequest request = null;

    private HttpMethod method = null;

    private String path = null;

    //private FileOutputQueue fileOutputQueue = null;
    
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

        Object message = e.getMessage();

        //System.out.printf( "GOT MESSAGE: %s\n", message.getClass().getName() );
        
        if ( message instanceof HttpRequest ) {

            this.request = (HttpRequest)message;
            this.method = request.getMethod();
            
            //TODO handle other methods other than GET here
            if ( method != PUT )  {
                sendError(ctx, METHOD_NOT_ALLOWED);
                return;
            }
          
            path = request.getUri();
            path = sanitizeUri( path );
            
            if ( path == null || ! path.startsWith( "/" ) ) {
                sendError(ctx, FORBIDDEN);
                return;
            }
            
            System.out.printf( "URL is: %s\n", path );
            
            if ( method == PUT ) {
                //fileOutputQueue = new FileOutputQueue( path );
                return;
            }

            if ( method == GET ) {

                //handle HTTP GETs here.
                
            }

        } else if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                //System.out.printf( "GOT chunk\n" );

                ChannelBuffer content = chunk.getContent();

                byte[] data = content.array();

                //System.out.printf( "%s\n", Hex.pretty( data ) );
                
                //fileOutputQueue.add( data );

            } else {

                //System.out.printf( "GOT LAST chunk\n" );

                //fileOutputQueue.add( new byte[0] );
                //fileOutputQueue.close();
                
                HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

                Channel ch = e.getChannel();

                ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);

            }

        }
            
    }

    private String sanitizeUri(String uri) throws java.net.URISyntaxException {

        // TODO: I believe this is actually wrong and that we have to try
        // ISO-8601 first according to the HTTP spec but I need to research this
        // problem.

        try {

            uri = URLDecoder.decode(uri, "UTF-8");

        } catch (UnsupportedEncodingException e) {

            try {
                uri = URLDecoder.decode(uri, "ISO-8859-1");
            } catch (UnsupportedEncodingException e1) {
                throw new Error();
            }

        }

        if ( uri.contains( "../" ) || uri.contains( "/.." ) )
            return null;

        // Convert to absolute path.
        return Config.PFS_ROOT + new URI( uri ).getPath();
        
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {
        
        Channel ch = e.getChannel();
        Throwable cause = e.getCause();

        cause.printStackTrace();

        if (cause instanceof TooLongFrameException) {
            sendError(ctx, BAD_REQUEST);
            return;
        }

        if (ch.isConnected()) {
            sendError(ctx, INTERNAL_SERVER_ERROR);
        }
        
    }

    private void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
        response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.setContent(ChannelBuffers.copiedBuffer(
                "Failure: " + status.toString() + "\r\n",
                CharsetUtil.UTF_8));

        // Close the connection as soon as the error message is sent.
        ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);

    }

}

