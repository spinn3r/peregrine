package peregrine.io.sstable;

/**
 *
 * A request for a set of keys or a scan request against the backend database from
 * the client.  This is provided as it allows us to keep track of whether request
 * was sent to the client (complete) and to get metadata about the client request.
 */
public abstract class BackendRequest {

    private ClientRequest client = null;

    private boolean complete = false;

    protected BackendRequest(ClientRequest client) {
        this.client = client;
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

}
