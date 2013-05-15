package peregrine.io.sstable;

import peregrine.StructReader;

/**
 *
 * A request for a set of keys or a scan request against the backend database from
 * the client.  This is provided as it allows us to keep track of whether request
 * was sent to the client (complete) and to get metadata about the client request.
 */
public abstract class BackendRequest implements Comparable<BackendRequest> {

    private ClientRequest client = null;

    private boolean complete = false;

    private StructReader seekKey;

    protected BackendRequest(ClientRequest client, StructReader seekKey ) {
        this.client = client;
        this.seekKey = seekKey;
    }

    public ClientRequest getClient() {
        return client;
    }

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    /**
     * The seekKey represents the entry that we're looking for with this
     * backend request.  For GET requests it's just the key for that entry.
     * For SCAN it's a bit different because we need to find that entry and
     * then change the seekKey if we suspend the request and then resume.
     */
    public StructReader getSeekKey() {
        return seekKey;
    }

    public void setSeekKey(StructReader seekKey) {
        this.seekKey = seekKey;
    }

    @Override
    public int compareTo(BackendRequest backendRequest) {
        return getSeekKey().compareTo( backendRequest.getSeekKey() );
    }

}
