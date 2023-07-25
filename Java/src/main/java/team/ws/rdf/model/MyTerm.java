package team.ws.rdf.model;

import java.util.HashSet;
import java.util.Objects;

public class MyTerm {
    public String iri;
    public String label;
    public int kind;  // IRI, blank node, literal：0, 1, 2
    public int term_id;
    public int edp;

    // IRI, blank node, literal, class, property, source, entity：0, 1, 2, 3, 4, 5, 6
    private final int IRI = 0;
    private final int BLANKNODE = 1;
    private final int LITERAL = 2;
    private final int CLASS = 3;
    private final int PROPERTY = 4;
    private final int SOURCE = 5;
    private final int ENTITY = 6;

    public boolean isClass = false;
    public boolean isProperty = false;
    public boolean isSource = false;

    public int count = 0;

    public HashSet<Integer> classes= new HashSet<>();
    public HashSet<Integer> forwardProperties= new HashSet<>();
    public HashSet<Integer> backwardProperties= new HashSet<>();

    public MyTerm() {
    }

    public MyTerm(String iri, String label, int kind, int term_id) {
        this.iri = iri;
        this.label = label;
        this.kind = kind;
        this.term_id = term_id;
    }

    public void setEdp(int edp) {
        this.edp = edp;
    }

    public int getEdp() {
        return edp;
    }

    public void setClass(boolean isClass) {
        this.isClass = isClass;
    }

    public void setProperty(boolean isProperty) {
        this.isProperty = isProperty;
    }

    public void setSource(boolean isSource) {
        this.isSource = isSource;
    }

    public boolean isClass() {
        return isClass;
    }

    public boolean isProperty() {
        return isProperty;
    }

    public boolean isSource() {
        return isSource;
    }

    public boolean isEntity() {
        return kind==IRI && !(isClass || isProperty || isSource);
    }

    public boolean isIri() {
        return kind == IRI;
    }

    public boolean isBlankNode() {
        return kind == BLANKNODE;
    }

    public boolean isLiteral() {
        return kind == LITERAL;
    }

    public int getSubKind() {
        if (kind != IRI) {
            return kind;
        }
        if (isClass) {
            return CLASS;
        } else if (isProperty) {
            return PROPERTY;
        } else if (isSource) {
            return SOURCE;
        }else {
            return ENTITY;
        }
    }

    public void addCount() {
        count += 1;
    }

    public int getCount() {
        return count;
    }

    public int getTerm_id() {
        return term_id;
    }

    public String getIri() {
        return iri;
    }

    public String getLabel() {
        return label;
    }

    public int getKind() {
        return kind;
    }

    public HashSet<Integer> getClasses() {
        return classes;
    }

    public HashSet<Integer> getBackwardProperties() {
        return backwardProperties;
    }

    public HashSet<Integer> getForwardProperties() {
        return forwardProperties;
    }

    public void addClass(int term_id) {
        this.classes.add(term_id);
    }

    public void addForwardProperty(int term_id) {
        this.forwardProperties.add(term_id);
    }

    public void addBackwardProperty(int term_id) {
        this.backwardProperties.add(term_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.iri, this.kind);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyTerm term = (MyTerm) o;
        return kind == term.kind && Objects.equals(iri, term.iri);
    }

    public String toString() {
        String base = String.format("%s  %s  %d  %d", this.iri, this.label, this.kind, this.term_id);
        if (isClass) {
            base += "  <c>";
        }
        if (isProperty) {
            base += "  <p>";
        }
        if (isSource) {
            base += "  <s>";
        }
        if (isEntity()) {
            base += "  <e>  " + classes.toString() + " ^ " + forwardProperties.toString() + "^" + backwardProperties.toString();
        }
        return base;
    }
}
