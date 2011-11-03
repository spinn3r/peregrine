package peregrine.io;

public final class ShuffleOutputReference implements OutputReference {

    private String name;

    public ShuffleOutputReference() {
        this( "default" );
    }

    public ShuffleOutputReference( String name ) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return "shuffle:" + getName();
    }
    
}
    
