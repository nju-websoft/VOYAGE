package team.ws.rdf.model;

public class MyVocabulary {
    public String namespace;
    public int vocabulary_id;
    public int count;

    public MyVocabulary(String namespace, int count, int vocabulary_id) {
        this.namespace = namespace;
        this.count = count;
        this.vocabulary_id = vocabulary_id;
    }

    public int getCount() {
        return count;
    }

    public int getVocabulary_id() {
        return vocabulary_id;
    }

    public String getNamespace() {
        return namespace;
    }
}
