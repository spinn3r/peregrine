package peregrine.shuffle.sender;

public class ShuffleFailedException extends Exception {

    public ShuffleFailedException( String message, Throwable cause ) {
        super( message, cause );
    }

}