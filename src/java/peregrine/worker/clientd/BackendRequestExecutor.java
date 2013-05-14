package peregrine.worker.clientd;

import com.spinn3r.log5j.Logger;
import org.jboss.netty.channel.Channel;
import peregrine.StructReader;
import peregrine.config.Config;
import peregrine.config.Partition;
import peregrine.io.SequenceWriter;
import peregrine.io.chunk.DefaultChunkWriter;
import peregrine.io.partition.LocalPartitionReader;
import peregrine.io.sstable.BackendRequest;
import peregrine.io.sstable.ClientRequest;
import peregrine.io.sstable.GetBackendRequest;
import peregrine.io.sstable.RecordListener;
import peregrine.io.util.Closer;
import peregrine.util.netty.NonBlockingChannelBufferWritable;

import java.io.IOException;
import java.util.*;

/**
 * Listens to the queue of items submitted by clients and executes them in a
 * background thread.
 *
 */
public class BackendRequestExecutor implements Runnable {

    protected static final Logger log = Logger.getLogger();

    private Config config;

    private BackendRequestQueue queue;

    private PartitionIndex partitionIndex = null;

    private Set<ClientRequest> clientIndex = null;

    public BackendRequestExecutor(Config config, BackendRequestQueue queue) {
        this.config = config;
        this.queue = queue;
    }

    /**
     * Return true if the given Channel is suspended and can not handle writes
     * without blocking.  This is an indication that the channel needs to suspend
     * so that the client can catch up on reads.  This might be a client lagging
     * or it might just have very little bandwidth.
     */
    public boolean isSuspended( Channel channel ) {

        return (channel.getInterestOps() & Channel.OP_WRITE) == Channel.OP_WRITE;

    }

    @Override
    public void run() {

        while( true ) {

            // http://mechanical-sympathy.blogspot.com/2011/10/smart-batching.html
            //
            // we have group 'commit' of requests for intelligent/smart
            // batching so that if we come BACK to the queue and it's full with
            // GET or SCAN requests that we can elide them so that blocks only need
            // to be decompressed for ALL inbound requests.

            List<GetBackendRequest> requests = new ArrayList( BackendRequestQueue.LIMIT );

            queue.drainTo(requests);

            // the list must be sorted before we service it so that we can
            // access keys on the same block without going backwards and
            // accessing the same block again.  A seek is involved with fetching
            // a block as is optionally decompression and that's expensive.  By
            // first sorting the set we avoid both expensive operations.
            Collections.sort( requests );

            // FIXME: make SURE that when we are handling adjacent keys
            // correctly and that when two clients each fetch the same key that
            // we handle that correctly.

            handle(requests);

        }

    }

    /**
     * Create a multi-level index of partitions -> files -> keys
     */
    private void createIndex( List<GetBackendRequest> requests ) {

        partitionIndex = new PartitionIndex();
        clientIndex = new HashSet<ClientRequest>();

        for( GetBackendRequest current : requests ) {

            ClientRequest clientRequest = current.getClient();

            // keep track of every client...
            clientIndex.add( clientRequest );

            SourceIndex sourceIndex = partitionIndex.fetch(clientRequest.getPartition());
            List<GetBackendRequest> list = sourceIndex.fetch( clientRequest.getSource());
            list.add( current );

        }

    }

    class PartitionIndex extends FetchIndex<Partition,SourceIndex> {

        @Override
        public SourceIndex newEntry() {
            return new SourceIndex();
        }
    }

    class SourceIndex extends FetchIndex<String,List<GetBackendRequest>> {

        @Override
        public List<GetBackendRequest> newEntry() {
            return new ArrayList<GetBackendRequest>();
        }

    }

    abstract class FetchIndex<K,V> {

        private Map<K,V> delegate = new HashMap();

        /**
         * Fetch the given key from this index, create a new entry if it isn't
         * already present.
         */
        public V fetch( K key ) {

            V entry = delegate.get( key );

            if ( entry == null ) {
                entry = newEntry();
                delegate.put( key, entry );
            }

            return entry;

        }

        public abstract V newEntry();

        public  Set<Map.Entry<K,V>> entries() {
            return delegate.entrySet();
        }

    }

    public void handle( List<GetBackendRequest> requests ) {

        // FIXME: what happens if we have two entries for the same key... we
        // should de-dup them but we have to be careful because two clients
        // could request the SAME key and we need to be careful and return it
        // correctly.  I could write a unit test for this but they would need to
        // come from different requests of course.

        // FIXME:if a key/value are BIGGER than the send buffer then we are
        // fucked and I think we will block?  What happens there? I need to
        // review the NIO netty code to see what it would do but I think it
        // goes into a queue.

        // we now need an index of partitions to scan , and then sources in those
        // partitions to scan and the keys in those files to scan.  Create the
        // index...

        createIndex( requests );

        // we have to first go through each partition, then each source, etc.
        for( Map.Entry<Partition,SourceIndex> partitionEntry : partitionIndex.entries() ) {

            Partition partition = partitionEntry.getKey();
            SourceIndex sourceIndex = partitionEntry.getValue();

            for( Map.Entry<String,List<GetBackendRequest>> pathEntry : sourceIndex.entries() ) {

                String source = pathEntry.getKey();
                List<GetBackendRequest> getBackendRequests = pathEntry.getValue();

                handle( partition, source, getBackendRequests );

            }

        }

        // now look at the requests and see which ones were NOT completed
        // and re-enqueue them... these are lagged clients so I don't think
        // we necessarily need to put them at the head of the queue.  We need to
        // look at all entries where the client is not CANCELLED and where the
        // entry is not complete

        List<GetBackendRequest> incomplete = new ArrayList<GetBackendRequest>();

        for( GetBackendRequest current : requests ) {

            if ( current.getClient().getState().equals( ClientRequest.State.CANCELLED ) ) {
                continue;
            }

            if ( current.isComplete() == false )
                incomplete.add( current );

        }

        // add them back into the queue.
        queue.add( incomplete );

    }

    /**
     * Handle an individual file no an specific partition.
     */
    public void handle( Partition partition, String source, List<GetBackendRequest> requests ) {

        try {

            LocalPartitionReader reader = null;

            try {

                log.info( "Handling %s on %s ", source, partition );

                //FIXME: we should support a table cache here...

                reader = new LocalPartitionReader( config, partition, source );

                // FIXME: another way to handle client overload would be to detect
                // when the TCP send buffer would be filled if we added say this key
                // and the next key THEN we can remove the keys and add then BACK on
                // the queue so that other requests get served in the mean time.  Of
                // course another isuse here is that if it's the ONLY client then
                // re-enqueing it again is just going to result in the SAME problem
                // happening all over again.

                // FIXME: add suspension here...


                // create a SequenceWriter for every client so that we have an
                // output channel.

                for( ClientRequest clientRequest : clientIndex ) {

                    NonBlockingChannelBufferWritable writable =
                            new NonBlockingChannelBufferWritable( clientRequest.getChannel() );

                    // FIXME: we need to set a mode here for the DefaultChunkWriter to
                    // include a CRC32 in the minimal form so that the entire record is
                    // checked for checksum.  For starters we need it for the wire
                    // protocol but we ALSO need it to detect if we failed to service
                    // the request.
                    DefaultChunkWriter writer = new DefaultChunkWriter( config , writable );

                    clientRequest.setSequenceWriter( writer );

                }

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


                reader.seekTo( requests, new RecordListener() {

                    @Override
                    public void onRecord( BackendRequest backendRequest, StructReader key, StructReader value ) {

                        ClientRequest clientRequest = backendRequest.getClient();

                        try {

                            SequenceWriter writer = clientRequest.getSequenceWriter();

                            writer.write( key, value );

                            // flush after every key write.  This allows us to serve
                            // requests with lower latency.  These go into the TCP
                            // send buffer immediately and sent ASAP.  Keeping
                            // them around in memory is just pointless
                            writer.flush();

                            // mark this key as
                            // served so that I can move on to other keys to
                            // serve after we execute the batch.  Both a scan AND
                            // a fetch may need to be suspended and when we
                            // resume we have to look at which requests are
                            // complete.
                            backendRequest.setComplete(true);

                            // FIXME: what happens if the key/value pair + length of
                            // both , can't actually fit in the TCP send buffer?
                            //
                            // FIXME: flag the client request as suspended if necessary.

                        } catch ( IOException e ) {

                            // mark this request as failed (cancel it) so that all
                            // future requests are simply skipped for this entry.

                            clientRequest.setState( ClientRequest.State.CANCELLED );

                            log.error("Could not write to client: ", e);

                        }

                    }

                } );

                // NOTE: the client writer should be closed BEFORE the reader so
                // that we can read all the values.  If we do the reverse we we will
                // segfault.

                for( ClientRequest clientRequest : clientIndex ) {

                    try {

                        SequenceWriter writer = clientRequest.getSequenceWriter();

                        writer.flush();
                        writer.close();

                    } catch ( IOException e ) {
                        log.error( "Unable to handle client: ", e );
                    }

                }

            } finally {
                new Closer( reader ).close();
            }

        } catch ( IOException e ) {

            // we can't notify clients that the request failed because it might
            // be already being served and further they were given an HTTP 200 OK
            // however the CRC of the response will fail.

            log.error( String.format( "Could not handle %s on %s", source, partition ), e );

        }

    }

}
