package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import peregrine.util.netty.*;
import peregrine.os.*;

import java.io.*;
import java.nio.channels.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

/**
 */
public class FSPutDirectHandler extends FSPutBaseHandler {

    public static byte[] EOF = new byte[0];
    
    private ChannelBufferWritable output = null;

    public FSPutDirectHandler( FSDaemon daemon, FSHandler handler ) throws IOException {
        super( handler );

        File file = new File( handler.path );
        
        // FIXME: this mkdir should be async.
        new File( file.getParent() ).mkdirs();

        MappedFile mappedFile = new MappedFile( daemon.getConfig(), file, "w" );

        output = mappedFile.getChannelBufferWritable();
        
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        super.messageReceived( ctx, e );
        
        Object message = e.getMessage();

        if ( message instanceof HttpChunk ) {

            HttpChunk chunk = (HttpChunk)message;

            if ( ! chunk.isLast() ) {

                output.write( chunk.getContent() );
                
            } else {

                // FIXME this must be async ... 
                output.close();
                
                // FIXME: I don't like how the status sending is decoupled from
                // pipeline requests AND non-pipeline requests.  I need to unify
                // these.

                if ( handler.remote == null ) {
                    HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
                    ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
                }

            }

        }
            
    }

}

