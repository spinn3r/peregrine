package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import peregrine.io.async.*;

/**
 */
public class FSPutDirectHandler extends FSPutBaseHandler {

    public static byte[] EOF = new byte[0];

    private FileChannel output = null;
    
    public FSPutDirectHandler( FSHandler handler ) throws IOException {
        super( handler );
        
        // used so that we can get our channel
        FileOutputStream out = new FileOutputStream( handler.path );
        
        output = out.getChannel();

    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        super.messageReceived( ctx, e );
        
        Object message = e.getMessage();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                ChannelBuffer content = chunk.getContent();    
                content.getBytes( 0, output, content.writerIndex() );

            } else {

                output.force( false );

                HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );

                ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);

            }

        }
            
    }

}

