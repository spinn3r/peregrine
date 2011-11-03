package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import peregrine.io.async.*;
import com.spinn3r.log5j.*;

/**
 */
public class FSPutDirectHandler extends FSPutBaseHandler {

    private static final Logger log = Logger.getLogger();

    public static byte[] EOF = new byte[0];

    private OutputStream asyncOutputStream = null;

    public FSPutDirectHandler( FSHandler handler ) {
        super( handler );
        
        asyncOutputStream = new AsyncOutputStream( handler.path );

    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        super.messageReceived( ctx, e );
        
        Object message = e.getMessage();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                ChannelBuffer content = chunk.getContent();
                byte[] data = content.array();

                asyncOutputStream.write( data );

            } else {

                asyncOutputStream.write( EOF );
                asyncOutputStream.close();

                HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );

                ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);

            }

        }
            
    }

}

