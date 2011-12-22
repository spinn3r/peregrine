package peregrine.io;

/**
 * Send output to nothing (AKA a blackhole).  Data goes in but it doesn't come
 * out.  This is like /dev/null on Unix/Linux.
 */
public final class BlackholeOutputReference implements OutputReference {

    @Override
    public String toString() {
        return "blackhole:";
    }
    
}
    
