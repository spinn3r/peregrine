package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import peregrine.io.partition.*;
import peregrine.util.*;

/**
 */
public class FSHeadDirectHandler extends SimpleChannelUpstreamHandler {

    private static ExecutorService executors =
        Executors.newCachedThreadPool( new DefaultThreadFactory( FSHeadDirectHandler.class) );
    
    private FSHandler handler;
    
    public FSHeadDirectHandler( FSHandler handler ) {
        this.handler = handler;
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        Channel ch = e.getChannel();

        executors.submit( new FSHeadDirectCallable( handler.path, ch ) );
        
    }

}

class FSHeadDirectCallable extends FSBaseDirectCallable {

    private String path;
    private Channel channel;
    
    public FSHeadDirectCallable( String path,
                                 Channel channel ) {
        this.path = path;
        this.channel = channel;
    }
    
    public Object call() throws Exception {

        if ( exists( channel, path ) ) {
            
            List<File> files = LocalPartition.getChunkFiles( path );

            int length = 0;
            
            for ( File file : files ) {
                length += file.length();
            }
            
            int nr_chunks = files.size();

            HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
            response.setHeader( "X-nr-chunks", "" + nr_chunks );
            response.setHeader( "X-length",    "" + length );
            
            channel.write(response);

        }

        return null;
        
    }
    
}