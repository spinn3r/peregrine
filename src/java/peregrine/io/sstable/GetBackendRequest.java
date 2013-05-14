package peregrine.io.sstable;

import peregrine.StructReader;

/**
 * Represents a request to the backend to GET a key.  This represents an
 * individual request for an item and not ALL the items in a GET client
 * request which may be numerous.
 */
public class GetBackendRequest extends BackendRequest implements Comparable<GetBackendRequest> {

    private StructReader key;

    public GetBackendRequest(ClientRequest client, StructReader key) {
        super( client );
        this.key = key;
    }

    public StructReader getKey() {
        return key;
    }

    @Override
    public int compareTo(GetBackendRequest getBackendRequest) {
        return key.compareTo( getBackendRequest.getKey() );
    }

}
