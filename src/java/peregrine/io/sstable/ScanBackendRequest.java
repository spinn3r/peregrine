package peregrine.io.sstable;

import peregrine.client.ScanRequest;

/**
 *
 */
public class ScanBackendRequest extends BackendRequest {

    private ScanRequest scanRequest = null;

    public ScanBackendRequest(ClientRequest client, ScanRequest scanRequest) {
        super(client);
        this.scanRequest = scanRequest;
    }

    public ScanRequest getScanRequest() {
        return scanRequest;
    }

}
