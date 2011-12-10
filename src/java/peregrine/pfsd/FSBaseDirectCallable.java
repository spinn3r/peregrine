package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.util.concurrent.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import com.spinn3r.log5j.Logger;

public abstract class FSBaseDirectCallable implements Callable {

    protected static final Logger log = Logger.getLogger();

    /**
     * If the given file exists, return true.  If not, return a NOT_FOUND and
     * return fals.e
     */
    public boolean exists( Channel channel, String path ) {

        File dir = new File( path );

        if ( ! dir.exists() ) {

            log.error( "Path does not exist: %s", path );
            
            HttpResponse response = new DefaultHttpResponse( HTTP_1_1, NOT_FOUND );
            channel.write(response).addListener(ChannelFutureListener.CLOSE);
            return false;

        }

        return true;
        
    }
    
}