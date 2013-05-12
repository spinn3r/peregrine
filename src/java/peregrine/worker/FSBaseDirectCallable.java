/*
 * Copyright 2011 Kevin A. Burton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package peregrine.worker;

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
     * return false.
     */
    public boolean exists( Channel channel, String path ) {

        //FIXME: we should NOT send an HTTP response as a side-effect of calling exists()
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
