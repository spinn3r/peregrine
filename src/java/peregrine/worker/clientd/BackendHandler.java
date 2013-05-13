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
package peregrine.worker.clientd;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import peregrine.*;
import peregrine.client.*;
import peregrine.config.*;
import peregrine.io.chunk.*;
import peregrine.io.partition.*;
import peregrine.io.sstable.*;
import peregrine.io.util.*;
import peregrine.shuffle.receiver.*;
import peregrine.util.*;
import peregrine.util.netty.*;
import peregrine.util.primitive.IntBytes;

import com.spinn3r.log5j.*;
import peregrine.worker.ErrorLoggingChannelUpstreamHandler;
import peregrine.worker.WorkerDaemon;

/**
 *
 * Handles interfacing with the tablet code and directly handling client requests
 * using the SSTableReader interface.
 * 
 */
public class BackendHandler extends ErrorLoggingChannelUpstreamHandler {

    protected static final Logger log = Logger.getLogger();

    private static Pattern PATH_REGEX =
        Pattern.compile( "/([0-9]+)/client-rpc/(GET|SCAN|MUTATE)" );

    private Config config;

    private String resource;

    private Partition partition = null;

    private WorkerDaemon daemon;

    public BackendHandler(Config config, WorkerDaemon daemon, String resource) throws Exception {

        this.config = config;
        this.daemon = daemon;
        this.resource = resource;

        Matcher matcher = PATH_REGEX.matcher( resource );

        if ( matcher.find() ) {
            partition = new Partition( Integer.parseInt( matcher.group( 1 ) ) );
        } 

    }

    /**
     * Return true if we can handle the given resource URL we were given.
     */
    public boolean handles() {
        return partition != null;
    }

    /**
     * Execute the given request directly.
     */
    public void exec( final Channel channel ) throws IOException {

        HttpResponse response = new DefaultHttpResponse( HTTP_1_1, OK );
        channel.write(response);

        GetRequest request = GetRequestURLParser.toRequest( resource );

        //FIXME: move ALL this code into the BackendRequestExecutor

        //daemon.getBackendRequestQueue().add();

        //FIXME: we should support a table cache here... 
        
        LocalPartitionReader reader = null;
        DefaultChunkWriter writer = null;

        //FIXME: we should have group 'commit' of requests for intelligent/smart
        //batching so that if we come BACK to the queue and it's full with
        //seekTo or scan requess that we can elide them so that blocks only need
        //to be decompressed for ALL inbound requests.

        // http://mechanical-sympathy.blogspot.com/2011/10/smart-batching.html

        // FIXME: what happens if we have two entries for the same key... we
        // should de-dup them but we have to be careful because two clients
        // could request the SAME key and we need to be careful and return it
        // correctly.

        // FIXME:if a key/value are BIGGER than the send buffer then we are
        // fucked and I think we will block?  What happens there?
        
        try {
            
            NonBlockingChannelBufferWritable writable = new NonBlockingChannelBufferWritable( channel );
            
            reader = new LocalPartitionReader( config, partition, request.getSource() );

            // FIXME: we need to set a mode here for the DefaultChunkWriter to
            // include a CRC32 in the minimal form so that the entire record is
            // checked for checksum.  For starters we need it for the wire
            // protocol but we ALSO need it to detect if we failed to service
            // the request. 
            writer = new DefaultChunkWriter( config , writable );

            // this is a bit of a hack so that we have a final writer to use
            // within the RecordListener
            final DefaultChunkWriter _writer = writer;
            final NonBlockingChannelBufferWritable _writable = writable;
            
            // FIXME: this must be in a dedicated thread!

            // FIXME: call these primary and secondary handlers... this will
            // make more sense moving forward.

            // FIXME: another way to handle client overload would be to detect
            // when the TCP send buffer would be filled if we added say this key
            // and the next key THEN we can remove the keys and add then BACK on
            // the queue so that other requests get served in the mean time.  Of
            // course another isuse here is that if it's the ONLY client then
            // re-enqueing it again is just going to result in the SAME problem
            // happening all over again.

            // FIXME:
            //
            // NOW that I know how to avoid channels that are not listening, Make it EASY to
            // skip over items that are NOT going to need responding.  
            //
            // First we need to look at the queue of requests and then build the plan for
            // fetching the keys.
            //
            // Then for EVERY block we are fetching from, we need to make sure it ACTUALLY has
            // keeps that we need to index.  Then we decompress it, and for EACH key we keep
            // making sure we actually need to fetch it. 
            //
            // Then at the END we need to look at the queue AGAIN to make sure we ahve fetched
            // everything.  We can use the changing interest ops to add them to the queue
            // again.
            //
            // ACTUALLY ... just iterate over ALL the keys on a per client basis.  THEN if a
            // client comes alive later we can service the keys at the trailing end of the
            // request and then come back and finish the request the next time around and give
            // him additional keys.
            
            reader.seekTo( request.getKeys(), new RecordListener() {

                    @Override
                    public void onRecord( StructReader key, StructReader value ) {

                        // FIXME: what happens if the key/value pair + length of
                        // both , can't actually fit in the TCP send buffer?

                        try {

                            _writer.write( key, value );
                            
                        } catch ( IOException e ) {

                            //FIXME: if we fail here we should abort ANY further
                            //requests to this client... I guess the best way to
                            //handle this is to mark this request as failed so
                            //that all future requests are simply skipped for
                            //this entry?  The problem is that techically we
                            //should completely abort the seekTo and re-scan the
                            //entries here OR support the ability to just have a
                            //Get request skipped externally but passing in more
                            //than StructReader but also a GetRequest of some
                            //sort.... The problem though is that we would need
                            //one per key not one per list of keys.
                            
                            log.error( "Could not write to client: " , e );

                            channel.close();

                        }

                    }
                    
                } );

            _writer.flush();

        } finally {
            // NOTE: the writer should be closed BEFORE the reader so that we
            // can read all the values.  If we do the revese we we will
            // segfault.
            new Closer( writer, reader ).close();
        }

    }
    
    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception {

        try {

            exec( e.getChannel() );

        } catch ( Exception exc ) {
            // catch all exceptions and then bubble them up.
            log.error( "Caught exception: ", exc );
            throw exc;
        }
            
    }

}

