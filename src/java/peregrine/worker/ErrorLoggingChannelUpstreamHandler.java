/*
 * Copyright 2011-2013 Kevin A. Burton
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.*;
import java.util.concurrent.*;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import peregrine.io.partition.*;
import peregrine.util.*;

import com.spinn3r.log5j.*;

/**
 * 
 */
public abstract class ErrorLoggingChannelUpstreamHandler extends SimpleChannelUpstreamHandler {

    protected static final Logger log = Logger.getLogger();

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
        throws Exception {

        log.error( "Caught exception: ", e.getCause() );

        super.exceptionCaught( ctx, e );
        
    }

}