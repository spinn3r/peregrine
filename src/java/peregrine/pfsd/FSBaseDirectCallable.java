package peregrine.pfsd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.util.concurrent.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

public abstract class FSBaseDirectCallable implements Callable {

    /**
     * If the given file exists, return true.  If not, return a NOT_FOUND and
     * return fals.e
     */
    public boolean exists( Channel channel, String path ) {

        File dir = new File( path );

        if ( ! dir.exists() ) {

            HttpResponse response = new DefaultHttpResponse( HTTP_1_1, NOT_FOUND );
            channel.write(response);
            return false;

        }

        return true;
        
    }
    
}