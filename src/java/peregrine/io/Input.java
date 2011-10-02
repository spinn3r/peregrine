package peregrine.io;

import java.util.*;

import peregrine.*;
import peregrine.util.*;
import peregrine.keys.*;

public final class Input {

    private List<InputReference> references = new ArrayList();

    public Input() { }

    public Input( String... paths ) {
        for( String path : paths ) {
            add( new FileInputReference( path ) );
        }
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

}