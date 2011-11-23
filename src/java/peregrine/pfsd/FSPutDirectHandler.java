package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.nio.channels.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

/**
 */
public class FSPutDirectHandler extends FSPutBaseHandler {

    public static byte[] EOF = new byte[0];

    public static boolean FORCE = false;
    
    private FileChannel output = null;

    public FSPutDirectHandler( FSDaemon daemon, FSHandler handler ) throws IOException {
        super( handler );

        // FIXME: ALL of this should be async... In fact ALL the IO here should
        // be async.
        new File( new File( handler.path ).getParent() ).mkdirs();
        
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

                // transferTo the data in this handler... 
                ChannelBuffer content = chunk.getContent();    
                content.getBytes( 0, output, content.writerIndex() );

            } else {

                if ( FORCE ) {

                    long before = System.currentTimeMillis();

                    output.force( true );

                    long after = System.currentTimeMillis();

                    log.info( "Took %,d ms to force output to disk." , (after-before) );

                }
                
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

