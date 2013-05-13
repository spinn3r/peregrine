package peregrine.io.sstable;

import peregrine.StructReader;

/**
 * Represents a request to the backend to GET a key.  This represents an
 * individual request for an item and not ALL the items in a GET client
 * request which may be numerous.
 */
public class GetBackendRequest implements Comparable<GetBackendRequest> {

    private StructReader key;

    private boolean complete = false;

    private ClientRequest client;

    public GetBackendRequest(ClientRequest client, StructReader key) {
        this.client = client;
        this.key = key;
    }

    public StructReader getKey() {
        return key;
    }

    /**
     *
     * @return true if this key has been successfully sent to the client.
     */
    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    public ClientRequest getClient() {
        return client;
    }

    @Override
    public int compareTo(GetBackendRequest getBackendRequest) {
        return key.compareTo( getBackendRequest.getKey() );
    }

}
