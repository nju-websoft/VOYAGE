package team.ws.rdf.dao;

import org.apache.jena.util.SplitIRI;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import team.ws.rdf.model.*;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;


public class RDFDao {
    private final JdbcTemplate jdbcTemplate;

    private final int batchSize;

    private static final Logger logger = Logger.getLogger(RDFDao.class);

    public RDFDao(JdbcTemplate jdbcTemplate, int batchSize) {
        this.jdbcTemplate = jdbcTemplate;
        this.batchSize = batchSize;
    }

    public int[] insertTerm(int dataset_id, int file_id, List<MyTerm> myTerms) {
        try{
            String sql = "INSERT INTO `rdf_term`(`dataset_id`,`file_id`,`iri`,`label`,`kind`,`sub_kind`,`term_id`) VALUES (?,?,?,?,?,?,?)";
            int[] ret = new int[]{};
            for (int batchIdx = 0; batchIdx <= myTerms.size()/batchSize; batchIdx++) {
                List<MyTerm> subMyTerms = myTerms.subList(batchIdx * batchSize, Math.min(myTerms.size(), (batchIdx+1)*batchSize));
                int[] subRet = this.jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        MyTerm data = subMyTerms.get(i);
                        preparedStatement.setInt(1, dataset_id);
                        preparedStatement.setInt(2, file_id);
                        preparedStatement.setString(3, data.getIri());
                        if (data.getLabel() == null) {
                            preparedStatement.setNull(4, Types.NULL);
                        } else {
                            preparedStatement.setString(4, data.getLabel());
                        }
                        preparedStatement.setInt(5, data.getKind());
                        preparedStatement.setInt(6, data.getSubKind());
                        preparedStatement.setInt(7, data.getTerm_id());
                    }
                    @Override
                    public int getBatchSize() {
                        return subMyTerms.size();
                    }
                });
                ret = ArrayUtils.addAll(ret, subRet);
            }
            logger.debug(String.format("file_id: %d, dataset_id: %d, insertTerm length: %d", file_id, dataset_id, ret.length));
            return ret;
        } catch (Exception e) {
            logger.error( file_id + " Insert Exception: ", e);
            return null;
        }
    }

    public int[] insertTriple(int dataset_id, int file_id, List<MyTriple> myTriples) {
        try {
            String sql = "INSERT INTO `rdf_triple`(`dataset_id`,`file_id`,`msg_code`,`subject`,`predicate`,`object`) VALUES (?,?,?,?,?,?)";
            int[] ret = new int[]{};
            for (int batchIdx = 0; batchIdx <= myTriples.size()/batchSize; batchIdx++) {
                List<MyTriple> subMyTriples = myTriples.subList(batchIdx * batchSize, Math.min(myTriples.size(), (batchIdx+1)*batchSize));
                int[] subRet = this.jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        MyTriple data = subMyTriples.get(i);
                        preparedStatement.setInt(1, dataset_id);
                        preparedStatement.setInt(2, file_id);
                        preparedStatement.setNull(3, Types.NULL);
                        preparedStatement.setInt(4, data.getSubject_id());
                        preparedStatement.setInt(5, data.getPredicate_id());
                        preparedStatement.setInt(6, data.getObject_id());
                    }
                    @Override
                    public int getBatchSize() {
                        return subMyTriples.size();
                    }
                });
                ret = ArrayUtils.addAll(ret, subRet);
            }
            logger.debug(String.format("file_id: %d, dataset_id: %d, insertTriple length: %d", file_id, dataset_id, ret.length));
            return ret;
        } catch (Exception e) {
            logger.error(file_id + " Insert Exception: ", e);
            return null;
        }
    }

    // valid type: class, property, source
    public int[] insertType(int dataset_id, int file_id, List<MyTerm> myTerms, String type) {
        if (!(type.equals("class") || type.equals("property") || type.equals("source"))) {
            System.out.println("INVALID TYPE in insertType()!");
            return null;
        }
        try {
            String sql = String.format("INSERT INTO `rdf_%s`(`dataset_id`,`file_id`,`%s_id`,`count`) VALUES (?,?,?,?)", type, type);
            int[] ret = new int[]{};
            for (int batchIdx = 0; batchIdx <= myTerms.size()/batchSize; batchIdx++) {
                List<MyTerm> subMyTerms = myTerms.subList(batchIdx * batchSize, Math.min(myTerms.size(), (batchIdx+1)*batchSize));
                int[] subRet = this.jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        MyTerm data = subMyTerms.get(i);
                        preparedStatement.setInt(1, dataset_id);
                        preparedStatement.setInt(2, file_id);
                        preparedStatement.setInt(3, data.getTerm_id());
                        preparedStatement.setInt(4, data.getCount());
                    }

                    @Override
                    public int getBatchSize() {
                        return subMyTerms.size();
                    }
                });
                ret = ArrayUtils.addAll(ret, subRet);
            }
            logger.debug(String.format("file_id: %d, dataset_id: %d, insertType(%s) length: %d", file_id, dataset_id, type, ret.length));
            return ret;
        } catch (Exception e) {
            logger.error(file_id + " Insert Exception: ", e);
            return null;
        }
    }

    public int[] insertEntity(int dataset_id, int file_id, List<MyTerm> myTerms) {
        try{
            String sql = "INSERT INTO `rdf_entity`(`dataset_id`,`file_id`,`entity_id`,`count`,`edp`) VALUES (?,?,?,?,?)";
            int[] ret = new int[]{};
            for (int batchIdx = 0; batchIdx <= myTerms.size()/batchSize; batchIdx++) {
                List<MyTerm> subMyTerms = myTerms.subList(batchIdx * batchSize, Math.min(myTerms.size(), (batchIdx+1)*batchSize));
                int[] subRet = this.jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        MyTerm data = subMyTerms.get(i);
                        preparedStatement.setInt(1, dataset_id);
                        preparedStatement.setInt(2, file_id);
                        preparedStatement.setInt(3, data.getTerm_id());
                        preparedStatement.setInt(4, data.getCount());
                        preparedStatement.setInt(5, data.getEdp());
                    }

                    @Override
                    public int getBatchSize() {
                        return subMyTerms.size();
                    }
                });
                ret = ArrayUtils.addAll(ret, subRet);
            }
            logger.debug(String.format("file_id: %d, dataset_id: %d, insertEntity length: %d", file_id, dataset_id, ret.length));
            return ret;
        } catch (Exception e) {
            logger.error(file_id + " Insert Exception: ", e);
            return null;
        }
    }

    public int[] insertEDP(int dataset_id, int file_id, List<int[]> edpRows) {
        try {
            String sql = "INSERT INTO `rdf_edp`(`dataset_id`,`file_id`,`edp_id`,`kind`,`term_id`) VALUES (?,?,?,?,?)";
            int[] ret = new int[]{};
            for (int batchIdx = 0; batchIdx <= edpRows.size()/batchSize; batchIdx++) {
                List<int[]> subEdpRows = edpRows.subList(batchIdx * batchSize, Math.min(edpRows.size(), (batchIdx+1)*batchSize));
                int[] subRet = this.jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        int[] data = subEdpRows.get(i);
                        preparedStatement.setInt(1, dataset_id);
                        preparedStatement.setInt(2, file_id);
                        preparedStatement.setInt(3, data[0]);
                        preparedStatement.setInt(4, data[1]);
                        preparedStatement.setInt(5, data[2]);
                    }
                    @Override
                    public int getBatchSize() {
                        return subEdpRows.size();
                    }
                });
                ret = ArrayUtils.addAll(ret, subRet);
            }
            logger.debug(String.format("file_id: %d, dataset_id: %d, insertEDP length: %d", file_id, dataset_id, ret.length));
            return ret;
        } catch (Exception e) {
            logger.error(file_id + " Insert Exception: ", e);
            return null;
        }
    }

    public int savePid(int dataset_id, int file_id, int term, int triple, String lang, String source) {
        if (triple <= 0) {
            return 0;
        }
        try {
            String sql = "INSERT INTO `rdf_pid` (`dataset_id`,`file_id`,`term`,`triple`,`lang`,`source`) VALUES (?,?,?,?,?,?)";
            logger.debug(String.format("[save pid] file_id: %d, dataset_id: %d, term: %d, triple: %d, lang: %s, source: %s", file_id, dataset_id, term, triple, lang, source));
            return jdbcTemplate.update(sql, dataset_id, file_id, term, triple, lang, source);
        } catch (Exception e) {
            logger.error(file_id + " Insert Exception: ", e);
            return 0;
        }
    }

    // return set of file_id
    public HashSet<Integer> selectPid() {
        try {
            String sql = "SELECT file_id FROM `rdf_pid`;";
            List<Integer> fileIdList = jdbcTemplate.queryForList(sql, Integer.class);
            return new HashSet<>(fileIdList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // return list of [file_id, dataset_id]
    public ArrayList<int[]> selectPidWithDatasetId() {
        try {
            String sql = "SELECT file_id, dataset_id FROM `rdf_pid`;";
            List<Map<String, Object>> queryList = jdbcTemplate.queryForList(sql);
            ArrayList<int[]> pidList = new ArrayList<>();
            for (Map<String, Object> qi : queryList) {
                int file_id = Integer.parseInt(qi.get("file_id").toString());
                int dataset_id = Integer.parseInt(qi.get("dataset_id").toString());
                pidList.add(new int[] {file_id, dataset_id});
            }
            return pidList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // return Map<file_id, download>, start<= file_id <= end
    public HashMap<Integer, String> getDownloadMap(int start, int end) {
        try {
            String sql = "SELECT file_id, download FROM `dump_file` WHERE file_id>=? AND file_id<=?;";
            List<Map<String, Object>> queryList = jdbcTemplate.queryForList(sql, start, end);
            HashMap<Integer, String> downloadMap = new HashMap<>();
            for (Map<String, Object> qi : queryList) {
                Integer file_id = Integer.parseInt(qi.get("file_id").toString());
                String download = (String) qi.get("download");
                downloadMap.put(file_id, download);
            }
            return downloadMap;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public int[] getFileIdRange(String source) {
        try {
            String sql = "SELECT min(file_id), max(file_id) FROM dump_file WHERE " +
                    "dataset_id >= (SELECT min(dataset_id) FROM dataset_metadata WHERE source=?) " +
                    "AND dataset_id <= (SELECT max(dataset_id) FROM dataset_metadata WHERE source=?);";
            Map<String, Object> map = jdbcTemplate.queryForMap(sql, source, source);
            return new int[]{Integer.parseInt(map.get("min(file_id)").toString()), Integer.parseInt(map.get("max(file_id)").toString())};
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<String> getIriListForVocabulary(int file_id) {
        try {
            String sql = "SELECT iri FROM `rdf_term` WHERE file_id=? AND (sub_kind=3 OR sub_kind=4);"; // class/property
            return jdbcTemplate.queryForList(sql, String.class, file_id);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public int[] insertVocabulary(int dataset_id, int file_id, List<MyVocabulary> myVocabularyList) {
        try {
            String sql = "INSERT INTO `rdf_vocabulary`(`dataset_id`,`file_id`,`vocabulary_id`,`namespace`,`count`) VALUES (?,?,?,?,?)";
            int[] ret = new int[]{};
            for (int batchIdx = 0; batchIdx <= myVocabularyList.size()/batchSize; batchIdx++) {
                List<MyVocabulary> subMyVocabularyList = myVocabularyList.subList(batchIdx * batchSize, Math.min(myVocabularyList.size(), (batchIdx+1)*batchSize));
                int[] subRet = this.jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        MyVocabulary data = subMyVocabularyList.get(i);
                        preparedStatement.setInt(1, dataset_id);
                        preparedStatement.setInt(2, file_id);
                        preparedStatement.setInt(3, data.getVocabulary_id());
                        preparedStatement.setString(4, data.getNamespace());
                        preparedStatement.setInt(5, data.getCount());
                    }
                    @Override
                    public int getBatchSize() {
                        return subMyVocabularyList.size();
                    }
                });
                ret = ArrayUtils.addAll(ret, subRet);
            }
            logger.debug(String.format("file_id: %d, dataset_id: %d, insertVocabulary length: %d", file_id, dataset_id, ret.length));
            return ret;
        } catch (Exception e) {
            logger.error(file_id + " Insert Exception: ", e);
            return null;
        }
    }

    // stream read
    public MsgData getMsgDataByFileIdStream(int file_id) {
        try {
            String sql;
            List<Map<String, Object>> queryList;
            sql = "SELECT term_id, iri, kind FROM `rdf_term` WHERE file_id=?;";
            queryList = jdbcTemplate.queryForList(sql, file_id);
            MyTerm[] myTerms = new MyTerm[queryList.size() + 1];
            for (Map<String, Object> qi : queryList) {
                int term_id = Integer.parseInt(qi.get("term_id").toString());
                String iri = (String) qi.get("iri");
                int kind = Integer.parseInt(qi.get("kind").toString());
                myTerms[term_id] = new MyTerm(iri, null, kind, term_id); // set all label = null
            }

            sql = "SELECT `subject`, predicate, object FROM rdf_triple WHERE file_id=" + file_id + ";";
            Set<MyTriple> allTriple = new HashSet<>();
            Map<Integer, Set<MyTriple>> bnode2triple = new HashMap<>();

            Connection con;
            Statement sta;
            ResultSet rs = null;
            con = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
            sta = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            sta.setFetchSize(Integer.MIN_VALUE); // Sets the number of rows that get multiple rows from the database
            sta.setFetchDirection(ResultSet.FETCH_REVERSE);
            rs = sta.executeQuery(sql);
            while (rs.next()) {
                int subject = rs.getInt("subject");
                int predicate = rs.getInt("predicate");
                int object = rs.getInt("object");
                MyTriple myTriple = new MyTriple(subject, predicate, object);
                allTriple.add(myTriple);
                if (myTerms[subject].isBlankNode()) {
                    Set<MyTriple> myTripleSet = bnode2triple.getOrDefault(subject, new HashSet<>());
                    myTripleSet.add(myTriple);
                    bnode2triple.put(subject, myTripleSet);
                }
                if (myTerms[object].isBlankNode()) {
                    Set<MyTriple> myTripleSet = bnode2triple.getOrDefault(object, new HashSet<>());
                    myTripleSet.add(myTriple);
                    bnode2triple.put(object, myTripleSet);
                }
            }
            return new MsgData(allTriple, bnode2triple, myTerms);
        } catch (Exception e) {
            logger.error("Exception in getMsgDataByFileId file_id=" + file_id + e.getMessage());
        }
        return null;
    }

    // insert triples into rdf_triple_copy1
    public int[] insertMsgCodeTriple(int dataset_id, int file_id, List<MyTriple> myTriples) {
        try {
            String sql = "INSERT INTO `rdf_triple_copy1`(`dataset_id`,`file_id`,`msg_code`,`subject`,`predicate`,`object`) VALUES (?,?,?,?,?,?)";
            int[] ret = new int[]{};
            for (int batchIdx = 0; batchIdx <= myTriples.size()/batchSize; batchIdx++) {
                List<MyTriple> subMyTriples = myTriples.subList(batchIdx * batchSize, Math.min(myTriples.size(), (batchIdx+1)*batchSize));
                int[] subRet = this.jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                        MyTriple data = subMyTriples.get(i);
                        preparedStatement.setInt(1, dataset_id);
                        preparedStatement.setInt(2, file_id);
                        preparedStatement.setString(3, data.getHash());
                        preparedStatement.setInt(4, data.getSubject_id());
                        preparedStatement.setInt(5, data.getPredicate_id());
                        preparedStatement.setInt(6, data.getObject_id());
                    }
                    @Override
                    public int getBatchSize() {
                        return subMyTriples.size();
                    }
                });
                ret = ArrayUtils.addAll(ret, subRet);
            }
            logger.debug(String.format("file_id: %d, dataset_id: %d, insertMsgCodeTriple length: %d", file_id, dataset_id, ret.length));
            return ret;
        } catch (Exception e) {
            logger.error(file_id + " insertMsgCodeTriple Exception: ", e);
            return null;
        }
    }

    // return set of file_id from table rdf_triple_copy1
    public HashSet<Integer> selectFileIdFromTripleCopy1() {
        try {
            String sql = "SELECT DISTINCT(file_id) FROM rdf_triple_copy1;";
            List<Integer> fileIdList = jdbcTemplate.queryForList(sql, Integer.class);
            return new HashSet<>(fileIdList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
