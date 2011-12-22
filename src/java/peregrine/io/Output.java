package peregrine.io;

import java.util.*;

public final class Output {

    private List<OutputReference> references = new ArrayList();

    public Output() { }

    public Output( List<String> paths ) {

        for( String path : paths ) {

            if ( path.contains( ":" ) ) {

                String[] split = path.split( ":" );

                String type      = split[0];
                String arg       = split[1];

                if ( "broadcast".equals( type ) )
                    add( new BroadcastOutputReference( arg ) );

                if ( "file".equals( type ) ) {
                    boolean append = split[2].equals( "true" );
                    add( new FileOutputReference( arg, append ) );
                }

                if ( "shuffle".equals( type ) )
                    add( new ShuffleOutputReference( arg ) );

               if ( "blackhole".equals( type ) )
                    add( new BlackholeOutputReference() );

            } else {
                add( new FileOutputReference( path ) );
            }

        }

    }
    
    public Output( String... paths ) {
        this( Arrays.asList( paths ) );
    }

    protected Output( OutputReference... refs ) {
        for( OutputReference ref : refs )
            add( ref );
    }
    
    protected Output( OutputReference ref ) {
        add( ref );
    }

    protected Output add( OutputReference ref ) {
        this.references.add( ref );
        return this;
    }

    public List<OutputReference> getReferences() {
        return references;
    }

    @Override
    public String toString() {
        return references.toString();
    }

}