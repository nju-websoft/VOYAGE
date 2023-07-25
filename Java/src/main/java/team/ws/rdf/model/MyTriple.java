package team.ws.rdf.model;

import java.util.Objects;

public class MyTriple {
    int subject_id;
    int predicate_id;
    int object_id;
    String hash;

    public MyTriple(int subject_id, int predicate_id, int object_id) {
        this.subject_id = subject_id;
        this.predicate_id = predicate_id;
        this.object_id = object_id;
        this.hash = null;
    }

    public int getObject_id() {
        return object_id;
    }

    public int getPredicate_id() {
        return predicate_id;
    }

    public int getSubject_id() {
        return subject_id;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public String getHash() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyTriple myTriple = (MyTriple) o;
        return subject_id == myTriple.subject_id && predicate_id == myTriple.predicate_id && object_id == myTriple.object_id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject_id, predicate_id, object_id);
    }

    @Override
    public String toString() {
        return "MyTriple{" +
                "subject_id=" + subject_id +
                ", predicate_id=" + predicate_id +
                ", object_id=" + object_id +
                '}';
    }
}
