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

import com.spinn3r.log5j.Logger;

/**
 */
public class FSDeleteDirectHandler extends SimpleChannelUpstreamHandler {

    protected static final Logger log = Logger.getLogger();

    private FSHandler handler;
    private FSDaemon daemon;
    private Channel channel;
    
    public FSDeleteDirectHandler( FSDaemon daemon, FSHandler handler ) {
        this.daemon = daemon;
        this.handler = handler;
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        channel = e.getChannel();

        daemon.getExecutorService( getClass() ).submit( new FSDeleteDirectCallable() );
        
    }

    class FSDeleteDirectCallable extends FSBaseDirectCallable {
        
        public Object call() throws Exception {

            int deleted = 0;

            // TODO: if the file does not exist should we return
            //
            // HTTP 404 File Not Found ?
            //
            // I think this would be the right thing to do
            //
            if ( exists( channel, handler.path ) ) {
                
                List<File> files = LocalPartition.getChunkFiles( handler.path );

                log.info( "Found %,d potential files to delete." );
                
                for( File file : files ) {

                    log.info( "Deleting %s", file.getPath() );

                    // TODO: use the new JDK 1.7 API so we can get the reason why the
                    // delete failed ... However, at the time this code was being
                    // written the delete() method did not exist in the Javadoc for 1.7
                    // so we can revisit it later once this problem has been sorted out.
                    
                    if ( ! file.delete() ) {

                        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, INTERNAL_SERVER_ERROR );
                        channel.write(response).addListener(ChannelFutureListener.CLOSE);
                        return null;
                        
                    }

                    ++deleted;
                    
                }

                HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
                response.setHeader( "X-deleted", "" + deleted );
                
                channel.write(response).addListener(ChannelFutureListener.CLOSE);

            }

            return null;
            
        }
        
    }
    
}

