package peregrine.shuffle.sender;

import java.io.IOException;

public class ShuffleFailedException extends IOException {

    public ShuffleFailedException( String message, Throwable cause ) {
        super( message, cause );
    }

}