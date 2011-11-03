package peregrine;

public interface Key {

    public byte[] toBytes();

    public void fromBytes( byte[] data );

}