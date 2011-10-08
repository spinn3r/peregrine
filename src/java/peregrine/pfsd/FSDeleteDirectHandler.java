package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.*;
import org.jboss.netty.handler.stream.*;
import org.jboss.netty.util.*;

import peregrine.*;
import peregrine.io.async.*;
import peregrine.io.partition.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 */
public class FSDeleteDirectHandler extends SimpleChannelUpstreamHandler {

    private static final Logger log = Logger.getLogger();

    private static ExecutorService executors =
        Executors.newCachedThreadPool( new DefaultThreadFactory( FSDeleteDirectHandler.class) );
    
    private FSHandler handler;
    
    public FSDeleteDirectHandler( FSHandler handler ) {
        this.handler = handler;
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        Channel ch = e.getChannel();

        executors.submit( new FSDeleteDirectCallable( handler.path, ch ) );
        
    }

}

class FSDeleteDirectCallable implements Callable {

    private static final Logger log = Logger.getLogger();

    private String path;
    private Channel channel;
    
    public FSDeleteDirectCallable( String path,
                                   Channel channel ) {
        this.path = path;
        this.channel = channel;
    }
    
    public Object call() throws Exception {

        // go through and delete everything.

        log.info( "Going to DELETE: %s" , path );

        File dir = new File( path );

        if ( ! dir.exists() ) {

            HttpResponse response = new DefaultHttpResponse( HTTP_1_1, NOT_FOUND );
            channel.write(response);
            return null;

        }
        
        List<File> files = LocalPartition.getChunkFiles( path );

        int deleted = 0;
        
        for( File file : files ) {

            // FIXME: use the new API so we can get the reason why the delete
            // failed ..
            if ( ! file.delete() ) {

                HttpResponse response = new DefaultHttpResponse( HTTP_1_1, INTERNAL_SERVER_ERROR );
                channel.write(response);
                return null;
                
            }

            ++deleted;
            
        }

        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
        response.setHeader( "X-deleted", "" + deleted );
        
        channel.write(response);

        return null;
        
    }
    
}