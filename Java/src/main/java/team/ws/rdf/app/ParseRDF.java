package team.ws.rdf.app;

import org.apache.jena.query.ARQ;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.util.FileManager;
import org.apache.log4j.Logger;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import team.ws.rdf.dao.RDFDao;
import team.ws.rdf.model.EDP;
import team.ws.rdf.model.MyTerm;
import team.ws.rdf.model.MyTriple;
import team.ws.rdf.service.MsgManager;
import team.ws.rdf.service.MyStreamRDF;
import team.ws.rdf.service.TermManager;

import java.io.*;
import java.util.*;

public class ParseRDF {
    private static final Logger logger = Logger.getLogger(ParseRDF.class);

    private static String ROOT;
    private static String SOURCE;
    private static String TEMP_NT; // .hdt to .nt
    private static int TID_BASE; // termId start default
    private static int FID_START; // this source file_id start
    private static int FID_END; // this source file_id end
    private static int BATCH_SIZE; // batch insert size
    private static Set<Integer> pidSet;
    private static Map<Integer, String> downloadMap; // key: file_id, value: download url
    private static final List<Integer> parseList = new ArrayList<>();
    private static final Set<Integer> skippedDatasetIdSet = new HashSet<>();

    private static int addEDP(Map<EDP, Integer> edpIntegerMap, MyTerm mt, int baseEdpId) {
        EDP edp = new EDP(baseEdpId + edpIntegerMap.size(), mt.getClasses(), mt.getForwardProperties(), mt.getBackwardProperties());
        if (!edpIntegerMap.containsKey(edp)) {
            edpIntegerMap.put(edp, edp.getEdp_id());
        }
        return edpIntegerMap.get(edp);
    }

    // return: next edp_id(for the compressed file)
    public static int classifyMyTerms(int dataset_id, int file_id, int baseEdpId, List<MyTerm> myTerms, RDFDao dao) {
        List<MyTerm> classTerms = new ArrayList<>();
        List<MyTerm> propertyTerms = new ArrayList<>();
        List<MyTerm> sourceTerms = new ArrayList<>();
        List<MyTerm> entityTerms = new ArrayList<>();

        Map<EDP, Integer> edpIntegerMap  = new HashMap<>();

        for (MyTerm mt : myTerms) {
            if (mt.isClass()) {
                classTerms.add(mt);
            }
            if (mt.isProperty()) {
                propertyTerms.add(mt);
            }
            if (mt.isSource()) {
                sourceTerms.add(mt);
            }
            if (mt.isEntity()) {
                int edp_id = addEDP(edpIntegerMap, mt, baseEdpId);
                mt.setEdp(edp_id);
                entityTerms.add(mt);
            }
        }

        List<int[]> edpRows = new ArrayList<>();
        for (EDP edp : edpIntegerMap.keySet()) {
            edpRows.addAll(edp.getEdpRows());
        }

        dao.insertType(dataset_id, file_id, classTerms, "class");
        dao.insertType(dataset_id, file_id, propertyTerms, "property");
        dao.insertType(dataset_id, file_id, sourceTerms, "source");
        dao.insertEntity(dataset_id, file_id, entityTerms);
        dao.insertEDP(dataset_id, file_id, edpRows);

        return baseEdpId + edpIntegerMap.size();
    }

    // return: next term_id, next edp_id, triple_count(for the compressed file)
    public static int[] readStreamRDF(int dataset_id, int file_id, String source, String path, String base, Lang lang,
                                      RDFDao dao, int baseTermId, int baseEdpId, boolean single) {
        InputStream in = FileManager.getInternal().open(path);
        if (in == null) {
            throw new IllegalArgumentException("File: " + path + " not found");
        }

        MyStreamRDF handler = new MyStreamRDF(baseTermId);
        RDFDataMgr.parse(handler, in, base, lang);

        List<MyTriple> myTriples = handler.getMyTriples();
        List<MyTerm> myTerms = new ArrayList<>(handler.getMyTerms());

        dao.insertTerm(dataset_id, file_id, myTerms);
        dao.insertTriple(dataset_id, file_id, myTriples);
        int nextBaseEdpId = classifyMyTerms(dataset_id, file_id, baseEdpId, myTerms, dao);
        if (single) { // single file save pid in this function, compressed file save in parseCompressedFile()
            int rows = dao.savePid(dataset_id, file_id, myTerms.size(), myTriples.size(), lang.getLabel(), source);
            if (rows > 0) {
                parseList.add(file_id);
            }
        }
        return new int[]{handler.getBaseTermId(), nextBaseEdpId, myTriples.size()};
    }

    public static Lang switchLang(String extension) {
        Lang lang = null;
        if (extension != null) {
            if (extension.equals("rdf") || extension.equals("rdfs") || extension.equals("owl") || extension.equals("xml")) {
                lang = Lang.RDFXML;
            } else if (extension.equals("ttl") || extension.equals("turtle")) {
                lang = Lang.TURTLE;
            } else if (extension.equals("n3") || extension.equals("nt") || extension.equals("ntriples")) {
                lang = Lang.NTRIPLES;
            } else if (extension.equals("nq") || extension.equals("nquads")) {
                lang = Lang.NQUADS;
            } else if (extension.equals("trix")) {
                lang = Lang.TRIX;
            } else if (extension.equals("jsonld")) {
                lang = Lang.JSONLD;
            } else if (extension.equals("json")) {
                lang = Lang.RDFJSON;
            }
        }
        return lang;
    }

    public static void parseSingleFile(String fileName, RDFDao dao) {
        int pointIdx = fileName.lastIndexOf('.');
        String name = fileName, extension = null;
        if (pointIdx >= 0) {
            name = fileName.substring(0, pointIdx);
            extension = fileName.substring(pointIdx+1).toLowerCase();
        }
        String[] strArr = name.split("_");
        int file_id = Integer.parseInt(strArr[0]), dataset_id = Integer.parseInt(strArr[1]);
        if (pidSet.contains(file_id)) {
            logger.debug(String.format("[pass] file_id: %d, dataset_id: %d", file_id, dataset_id));
            return;
        }
        String path = ROOT + fileName;
        Lang lang = switchLang(extension);
        if (lang == null) { // hdt: lang == null || path == null
            return;
        }
        try {
            readStreamRDF(dataset_id, file_id, SOURCE, path, downloadMap.getOrDefault(file_id, null), lang, dao, TID_BASE, TID_BASE, true);
        } catch (Exception e) {
            logger.error(String.format("%s (file_id: %d, dataset_id: %d) ERROR in parseSingleFile: %s", fileName, file_id, dataset_id, e.getMessage()));
        }
    }

    // param: dataset_id, file_id, baseTermId, baseEdpId
    // return: next term_id, next edp_id, triple_count
    public static int[] parseDirectory(String directoryPath, RDFDao dao, String base, int[] param) {
        int dataset_id = param[0], file_id = param[1], baseTermId = param[2], baseEdpId = param[3], tripleCnt = 0;
        File directory = new File(directoryPath);
        String []filename = directory.list();
        assert filename != null;
        for (String s : filename) {
            String path = directoryPath + "/" + s;
            File file = new File(path);
            if (file.isDirectory()) {
                int[] ret = parseDirectory(path, dao, base, new int[]{dataset_id, file_id, baseTermId, baseEdpId});
                baseTermId = ret[0];
                baseEdpId = ret[1];
                tripleCnt += ret[2];
            } else {
                int pointIdx = s.lastIndexOf('.');
                String extension = null;
                if (pointIdx >= 0) {
                    extension = s.substring(pointIdx+1).toLowerCase();
                }
                Lang lang = switchLang(extension);
                if (lang == null) {
                    continue;
                }
                try {
                    int[] ret = readStreamRDF(dataset_id, file_id, SOURCE, path, base, lang, dao, baseTermId, baseEdpId, false);
                    baseTermId = ret[0];
                    baseEdpId = ret[1];
                    tripleCnt += ret[2];
                } catch (Exception e) {
                    logger.error(String.format("%s (file_id: %d, dataset_id: %d) ERROR in parseDirectory: %s", s, file_id, dataset_id, e.getMessage()));
                }
            }
        }
        return new int[]{baseTermId, baseEdpId, tripleCnt};
    }

    public static void parseCompressedFile(String directoryName, RDFDao dao) {
        String[] strArr = directoryName.split("_");
        int file_id = Integer.parseInt(strArr[0]), dataset_id = Integer.parseInt(strArr[1]);
        if (pidSet.contains(file_id)) {
            logger.debug(String.format("[pass] file_id: %d, dataset_id: %d", file_id, dataset_id));
            return;
        }
        int baseTermId = TID_BASE, baseEdpId = TID_BASE;
        String base = downloadMap.getOrDefault(file_id, null);
        int[] ret = parseDirectory(ROOT+directoryName, dao, base, new int[]{dataset_id, file_id, baseTermId, baseEdpId});
        int term = ret[0] - 1, triple = ret[2];
        int idx1 = directoryName.lastIndexOf('.'), idx2 = directoryName.lastIndexOf("_files");
        if (idx2 < idx1+1) {
            idx2 = directoryName.length();
        }
        String lang = directoryName.substring(idx1+1, idx2).toUpperCase();
        int rows = dao.savePid(dataset_id, file_id, term, triple, lang, SOURCE);
        if (rows > 0) {
            parseList.add(file_id);
        }
    }

    public static RDFDao loadProperties(boolean server) {
        Properties props = new Properties();
        try {
            props.load(ParseRDF.class.getResourceAsStream("/parse.properties"));

            ROOT = props.getProperty("root");
            if (server) {
                ROOT = props.getProperty("server.root");
                ARQ.init();
            }
            SOURCE = props.getProperty("source");
            TEMP_NT = props.getProperty("temp_nt");
            TID_BASE = Integer.parseInt(props.getProperty("tid_base"));
            BATCH_SIZE = Integer.parseInt(props.getProperty("batch_size"));

            System.setProperty("entityExpansionLimit", props.getProperty("entityExpansionLimit"));

            DriverManagerDataSource dataSource = new DriverManagerDataSource();
            dataSource.setDriverClassName(props.getProperty("DriverClassName"));
            String host = props.getProperty("serverHost"), port = props.getProperty("serverPort"),
                    database = props.getProperty("serverDatabase"), parameter = props.getProperty("MysqlParameter");
            dataSource.setUrl(String.format("jdbc:mysql://%s:%s/%s?%s", host, port, database, parameter));
            String username = props.getProperty("serverUsername"), password = props.getProperty("serverPassword");
            dataSource.setUsername(username);
            dataSource.setPassword(password);
            JdbcTemplate serverJdbcTemplate = new JdbcTemplate(dataSource);

            RDFDao dao = new RDFDao(serverJdbcTemplate, BATCH_SIZE);
            pidSet = dao.selectPid();
            int[] fileIdRange = dao.getFileIdRange(SOURCE);
            FID_START = fileIdRange[0];
            FID_END = fileIdRange[1];
            downloadMap = dao.getDownloadMap(FID_START, FID_END);

            String skipped = props.getProperty("skipped_dataset_id");
            for (String s: skipped.split(", ")) {
                skippedDatasetIdSet.add(Integer.parseInt(s));
            }

            return dao;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void parseFiles(RDFDao dao) {
        logger.debug("================= Now parsing =================");

        File f = new File(ROOT);
        String []filename = f.list();
        assert filename != null;
        for (int i=0; i<filename.length; i++) {
            System.out.printf("Now parsing %d/%d  %s%n", i, filename.length, filename[i]);

            String[] strArr = filename[i].split("_");
            int dataset_id = Integer.parseInt(strArr[1]);
            if (skippedDatasetIdSet.contains(dataset_id)) {
                logger.debug(String.format("[skip] dataset_id: %d (%s) since triples too large", dataset_id, filename[i]));
            }

            File file = new File(ROOT+filename[i]);
            if (file.isFile()) {
                parseSingleFile(filename[i], dao);
            } else {
                parseCompressedFile(filename[i], dao);
            }
        }
        logger.debug(String.format("parsed files: %d, total parsed: %d \n%s", parseList.size(), parseList.size()+pidSet.size(), parseList));
        logger.debug(String.format("skipped dataset_id set: \n%s", skippedDatasetIdSet));
    }

    public static void handleVocabularies(RDFDao dao) {
        logger.debug("================= Now handing vocabularies =================");

        List<int[]> pidList = dao.selectPidWithDatasetId();
        parseList.clear();
        for (int i = 0; i < pidList.size(); i++) {
            int file_id = pidList.get(i)[0], dataset_id = pidList.get(i)[1];
            System.out.printf("Now handling vocabulary %d/%d  file_id: %d, dataset_id: %d\n", i, pidList.size(), file_id, dataset_id);
            TermManager tm = new TermManager(dataset_id, file_id);
            tm.handlerVocabularies(dao);
            parseList.add(file_id);
        }
        logger.debug(String.format("handled vocabulary files: %d\n%s", parseList.size(), parseList));
    }

    public static void hashTriples(RDFDao dao) {
        logger.debug("================= Now hashing triples =================");

        List<int[]> pidList = dao.selectPidWithDatasetId();
        HashSet<Integer> processedSet = new HashSet<>();
        processedSet = dao.selectFileIdFromTripleCopy1();
        for (int i = 0; i < pidList.size(); i++) {
            int file_id = pidList.get(i)[0], dataset_id = pidList.get(i)[1];
            System.out.printf("Now hashing %d/%d  file_id: %d, dataset_id: %d\n", i + 1, pidList.size(), file_id, dataset_id);
            if (processedSet.contains(file_id)) {
                continue;
            }
            try {
                MsgManager.hashTriplesByFileId(dao, dataset_id, file_id);
            } catch (Exception e) {
                logger.error("hashTriples exception when hash file_id=" + file_id  + "\nhashed: " + processedSet + e.getMessage());
            }
            processedSet.add(file_id);
        }
        logger.debug(String.format("hashed files: %d\n%s", processedSet.size(), processedSet));
    }


    public static void main(String[] args) {
        RDFDao dao = loadProperties(false);
        assert dao != null;

        parseFiles(dao);
        handleVocabularies(dao);
        hashTriples(dao);
    }
}
