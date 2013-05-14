package peregrine.client;

import peregrine.Record;

import java.io.IOException;
import java.util.List;

/**
 * Client interface which represents the functionality that each client must
 * implement.
 */
public interface Client {

    /**
     * Wait for the server to respond.
     */
    public void waitForResponse() throws IOException;

    /**
     * Wait for all records to be parsed.
     * @throws IOException
     */
    public void waitFor() throws IOException;

    /**
     * Get the resulting records.
     */
    public List<Record> getRecords();

    /**
     * The underlying connection for this client.
     */
    public Connection getConnection();

}
