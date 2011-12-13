package peregrine.io;

import java.util.*;

public final class Input {

    private List<InputReference> references = new ArrayList();

    public Input() { }

    public Input( List<String> paths ) {

        for( String path : paths ) {

            if ( path.contains( ":" ) ) {

                String[] split = path.split( ":" );

                if ( split.length < 2 )
                    throw new RuntimeException( "Unable to split: " + path );
                
                String type      = split[0];
                String arg       = split[1];

                if ( "broadcast".equals( type ) )
                    add( new BroadcastInputReference( arg ) );

                if ( "file".equals( type ) )
                    add( new FileInputReference( arg ) );

                if ( "shuffle".equals( type ) )
                    add( new ShuffleInputReference( arg ) );

            } else { 
                add( new FileInputReference( path ) );
            }

        }

    }
    
    public Input( String... paths ) {

        this( Arrays.asList( paths ) );
        
    }

    public Input( InputReference... refs ) {
        for( InputReference ref : refs )
            add( ref );
    }

    public Input( InputReference ref ) {
        add( ref );
    }
    
    public Input add( InputReference ref ) {
        this.references.add( ref );
        return this;
    }

    public List<InputReference> getReferences() {
        return references;
    }

    @Override
    public String toString() {
        return references.toString();
    }
    
}