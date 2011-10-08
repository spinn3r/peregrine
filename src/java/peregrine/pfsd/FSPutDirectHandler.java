package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.net.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.*;
import org.jboss.netty.handler.stream.*;
import org.jboss.netty.util.*;

import peregrine.*;
import peregrine.io.async.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 */
public class FSPutDirectHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    public static byte[] EOF = new byte[0];

    private HttpRequest request = null;

    private OutputStream asyncOutputStream = null;

    /**
     * NR of bytes written.
     */
    private long written = 0;

    /**
     * Time we started the request.
     */
    private long started;

    /**
     * Number of chunks written.
     */
    private long chunks = 0;

    private FSHandler handler;
    
    public FSPutDirectHandler( FSHandler handler ) {
        this.handler = handler;

        started = System.currentTimeMillis();
        
        asyncOutputStream = new AsyncOutputStream( handler.path );

    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        Object message = e.getMessage();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                ChannelBuffer content = chunk.getContent();
                byte[] data = content.array();

                written += data.length;
                chunks = chunks + 1;

                asyncOutputStream.write( data );

            } else {

                asyncOutputStream.write( EOF );
                asyncOutputStream.close();

                // log that this was written successfully including the NR of
                // bytes.

                long duration = System.currentTimeMillis() - started;

                int mean_chunk_size = 0;

                if ( chunks > 0 )
                    mean_chunk_size = (int)(written / chunks);
                
                log.info( "Wrote %,d bytes in %,d chunks (mean chunk size = %,d bytes) in %,d ms to %s",
                          written, chunks, mean_chunk_size, duration, handler.path );
                
                HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );

                Channel ch = e.getChannel();

                ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);

            }

        }
            
    }

}

