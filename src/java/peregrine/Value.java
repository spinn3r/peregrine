package peregrine;

public interface Value {

    public byte[] toBytes();

    public void fromBytes( byte[] data );
    
}