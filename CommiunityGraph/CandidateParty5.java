package org.wso2.siddhi.extension.TwitterSentiment;

import static java.util.concurrent.TimeUnit.HOURS;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;

/*
 #TweetReader:getPartyNew2(from_user,5)
 @Param: UserScreenName and Executor Schedule Time
 @Return:Highest Percentage Party and That percentage
 */
public class CandidateParty5 extends StreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CandidateParty5.class);
    ComplexEventChunk<StreamEvent> returnEventChunk;
    VariableExpressionExecutor variableExpressionURLName;
    private static String[] Trump = { "trump", "donaldtrump", "trump2016", "makeamericagreatagain", "realDonaldTrump",
        "trumpforpresident", "trumppresident2016", "donaldtrumpforpresident", "donaldtrumpforpresident2016",
        "buttrump", "WomenForTrump" };
    private static String[] Clinton = { "hillary2016", "hillaryclinton", "hillaryforpresident2016", "imwithher",
            "hillaryforpresident", "hillary", "HillYes" };
    private static String[] Bernie = { "bernie2016", "feelthebern", "berniesanders", "bernie", "bernieforpresident",
            "bernieorbust", "bernbots", "berniebros" ,"bernsreturns"};
    private static String[] Ted = { "tedcruz", "cruzcrew", "cruz2016", "makedclisten", "cruzcrew", "choosecruz",
            "tedcruzforpresident", "tedcruz2016", "istandwithtedcruz", "cruztovictory" };
    private static int ShTm = 2;
    private static int corePoolSize = 8;
    private static int maxPoolSize = 400;
    private static long keepAliveTime = 20000;
    private static int jobQueueSize = 10000;
    private static ConcurrentHashMap hm;
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static BlockingQueue<Runnable> bounded;
    private static ExecutorService threadExecutor;

    @Override
    public void start() {
        bounded = new LinkedBlockingQueue<Runnable>(jobQueueSize);
        threadExecutor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS,
                bounded);
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (logger.isInfoEnabled()) {
                    logger.info("Starting... ");
                }
                if (hm != null) {
                    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    Date now = new Date();
                    String strDate = sdfDate.format(now);
                    Database database = new Database();
                    Set hmapset = hm.keySet();
                    java.sql.Connection connection = null;
                    java.sql.Statement stmt = null;
                    try {
                        connection = database.Get_Connection();
                        stmt = connection.createStatement();
                        for (int i = 0; i < hm.size(); i++) {
                            String query = "select * from tweep where name =?";
                            java.sql.PreparedStatement st = connection.prepareStatement(query);
                            st.setString(1, hmapset.toArray()[i].toString());
                            java.sql.ResultSet resultSet = st.executeQuery();
                            if (!resultSet.next()) {
                                if (hm.get(hmapset.toArray()[i].toString()) != "T") {
                                    String sql = "INSERT INTO tweep VALUES ('" + hmapset.toArray()[i].toString()
                                            + "','" + strDate + "','" + hm.get(hmapset.toArray()[i].toString()) + "')";
                                    stmt.addBatch(sql);
                                }
                            }
                        }
                        stmt.executeBatch();
                        if (logger.isInfoEnabled()) {
                            logger.info("Finished Coppied... ");
                        }
                    } catch (SQLException e) {
                        logger.error("Error Enter DB ", e.getMessage());
                    } catch (Exception e) {
                        logger.error("Error Enter DB ", e.getMessage());
                    } finally {
                        try {
                            stmt.close();
                            connection.close();
                        } catch (SQLException e) {
                            logger.error("Error CloseDB ", e.getMessage());
                        }
                    }
                }
                hm = new ConcurrentHashMap();
            }
        }, 0, ShTm, HOURS);

    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub
        scheduler.shutdownNow();
        threadExecutor.shutdownNow();
        bounded.clear();

    }

    @Override
    public Object[] currentState() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void restoreState(Object[] state) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        // TODO Auto-generated method stub
        returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            if (hm.containsKey((String) variableExpressionURLName.execute(streamEvent))) {
                if (hm.get((String) variableExpressionURLName.execute(streamEvent)) != "T") {
                    complexEventPopulater.populateComplexEvent(streamEvent,
                            new Object[] { hm.get((String) variableExpressionURLName.execute(streamEvent)), 0 });
                    returnEventChunk.add(streamEvent);
                }
            } else {
                hm.put((String) variableExpressionURLName.execute(streamEvent), "T");
                threadExecutor.submit(new jsoupConnection(streamEvent, complexEventPopulater));
            }
        }
        sendEventChunk();
    }

    public synchronized void addEventChunk(StreamEvent event) {
        returnEventChunk.add(event);
    }

    public synchronized void sendEventChunk() {
        if (returnEventChunk.hasNext()) {
            nextProcessor.process(returnEventChunk);
            returnEventChunk = new ComplexEventChunk<StreamEvent>();
        }
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (!(attributeExpressionExecutors.length == 2)) {
            throw new UnsupportedOperationException("Invalid number of Arguments");
        }
        if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            variableExpressionURLName = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        }
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new UnsupportedOperationException("Required a variable, but found a otherparameter");
        } else {
            ShTm = (Integer) attributeExpressionExecutors[1].execute(null);
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("TopName", Attribute.Type.STRING));
        attributeList.add(new Attribute("Top", Attribute.Type.DOUBLE));
        return attributeList;

    }

    class jsoupConnection implements Runnable {
        String Username;
        StreamEvent event;
        ComplexEventPopulater complexEventPopulater;
        public jsoupConnection(StreamEvent event, ComplexEventPopulater complexEventPopulater) {
            this.Username = (String) variableExpressionURLName.execute(event);
            this.event = event;
            this.complexEventPopulater = complexEventPopulater;
        }

        @Override
        public void run() {
            // TODO Auto-generated method stub
            String TweetChunk = "";
            double TrumpCount = 0, BernieCount = 0, ClintonCount = 0, TedCount = 0;
            String URL = "https://twitter.com/" + Username;
            Document doc;
            int maxeve = 0;
            try {
                Connection con = Jsoup
                        .connect(URL)
                        .userAgent(
                                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21")
                        .ignoreContentType(true).ignoreHttpErrors(true).timeout(10000);;
                doc = con.get();
                Elements paragraphs = doc.select("b");
                for (Element p : paragraphs) {
                    TweetChunk = TweetChunk.concat(p.text().concat(" "));
                    if (maxeve > 50)
                        break;
                    maxeve++;
                }
                for (String t : Trump) {
                    TrumpCount = TrumpCount + StringUtils.countMatches(TweetChunk.toLowerCase(), t.toLowerCase());
                }
                for (String t : Bernie) {
                    BernieCount = BernieCount + StringUtils.countMatches(TweetChunk.toLowerCase(), t.toLowerCase());
                }
                for (String t : Clinton) {
                    ClintonCount = ClintonCount + StringUtils.countMatches(TweetChunk.toLowerCase(), t.toLowerCase());
                }

                for (String t : Ted) {
                    TedCount = TedCount + StringUtils.countMatches(TweetChunk.toLowerCase(), t.toLowerCase());
                }
                double sum = TrumpCount + BernieCount + ClintonCount + TedCount + 1;
                // System.out.println("Tot="+tot);
                List<Candidate5> list = new ArrayList<Candidate5>();
                list.add(new Candidate5("CLINTON", (double) ((ClintonCount / sum) * 100.00)));
                list.add(new Candidate5("TRUMP", (double) ((TrumpCount / sum) * 100.00)));
                list.add(new Candidate5("BERNIE", (double) ((BernieCount / sum) * 100.00)));
                list.add(new Candidate5("CRUZ", (double) ((TedCount / sum) * 100.00)));
                Collections.sort(list, new Candidate5());
                if (hm.containsKey((String) variableExpressionURLName.execute(event))) {
                    if (hm.get((String) variableExpressionURLName.execute(event)) == "T") {
                        hm.replace((String) variableExpressionURLName.execute(event), new String(list.get(0)
                                .getCandidateName()));
                    }
                }
                if (sum != 1) {
                    complexEventPopulater.populateComplexEvent(event, new Object[] { list.get(0).getCandidateName(),
                            list.get(0).getCandidateRank() });
                } else {
                    complexEventPopulater.populateComplexEvent(event, new Object[] { "Other", 0 });
                }
                addEventChunk(event);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                // e.printStackTrace();
                logger.error("Error Connecting Twitter API  ", e.getMessage());
            }

        }

    }

}

class Candidate5 implements Comparator<Candidate5>, Comparable<Candidate5> {
    String name;
    double rank;

    Candidate5() {
    }

    Candidate5(String n, double a) {
        name = n;
        rank = a;
    }

    public String getCandidateName() {
        return name;
    }

    public double getCandidateRank() {
        return rank;
    }

    // Overriding the compareTo method
    public int compareTo(Candidate5 d) {
        return (this.name).compareTo(d.name);
    }

    // Overriding the compare method to sort the age
    public int compare(Candidate5 d, Candidate5 d1) {
        return (int) (d1.rank - d.rank);
    }
}

class Database {

    public java.sql.Connection Get_Connection() throws Exception {
        try {
            String connectionURL = "jdbc:mysql://localhost:3306/ElectionCEP";
            java.sql.Connection connection = null;
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            connection = (java.sql.Connection) DriverManager.getConnection(connectionURL, "root", "mLsxACaH4GECQ");
            return connection;
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw e;
        }
    }

}
