package team.ws.rdf.service;

import org.apache.jena.rdf.model.impl.Util;
import org.apache.jena.util.SplitIRI;
import team.ws.rdf.dao.RDFDao;
import team.ws.rdf.model.MyVocabulary;

import java.util.*;

public class TermManager {
    private final int file_id;
    private final int dataset_id;

    public HashMap<String, Integer> vocabularyMap = new HashMap<>();

    public TermManager(int dataset_id,int file_id) {
        this.file_id = file_id;
        this.dataset_id = dataset_id;
    }

    public void addVocabulary(String s) {
        String ns;
        try {
            ns = s.substring(0, Util.splitNamespaceXML(s));
        } catch (Exception e) {
            ns = SplitIRI.namespace(s);
        }
        int val = vocabularyMap.getOrDefault(ns, 0);
        vocabularyMap.put(ns, val + 1);
    }

    public void handlerVocabularies(RDFDao dao) {
        List<String> termList = dao.getIriListForVocabulary(file_id);
        for (String term: termList) {
            addVocabulary(term);
        }

        int nsId = 1;
        List<MyVocabulary> myVocabularyList = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : vocabularyMap.entrySet()) {
            myVocabularyList.add(new MyVocabulary(entry.getKey(), entry.getValue(), nsId));
            nsId += 1;
        }
        dao.insertVocabulary(dataset_id, file_id, myVocabularyList);
    }
}
