package org.wso2.siddhi.extension.TwitterSentiment;

import java.io.IOException;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
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

public class CandidateParty2 extends StreamProcessor {
    private static final org.slf4j.Logger logger =  LoggerFactory.getLogger(CandidateParty2.class);
    private VariableExpressionExecutor variableExpressionURLName;
    private int date = 1;
    private static String[] Trump = { "#trump", "#donaldtrump", "#trump2016", "#makeamericagreatagain",
        "#realDonaldTrump", " #TRUMP" };
    private static String[] Clinton = { "#hillary2016", "#hillaryclinton", "#hillaryforpresident2016", "#imwithher",
            "#hillaryforpresident", "#hillary", "#HillYes" };
    private static String[] Bernie = { "#bernie2016", "#feelthebern", "#berniesanders", "#bernie", "#wearebernie",
            "#berniesandersforpresident2016", "#berniesandersforpresident", "#bernieforpresident2016",
            "#bernieforpresident", "#bernieorbust", "#bernbots", "#berniebros" };
    private static String[] Ted = { "#tedcruz", "#cruzcrew", "#cruz2016", " #makedclisten", "#cruzcrew", "#choosecruz",
            "#tedcruzforpresident", "#tedcruzforpresident2016", "#tedcruz2016", "#istandwithtedcruz", "#cruztovictory" };
    private static String[] Marco = { "#marcorubio", " #teamrubio", " #norubio", "#rubio2016", "#studentsforrubio",
            "#gopdebate", "#rubio", "#marco", "#rubioforpresident", "#rubioforpresident2016" };

    @Override
    public void start() {
        // TODO Auto-generated method stub

    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub

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
        String URL;
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;  
        long lStartTime = System.currentTimeMillis();
        TwitterCriteria12 criteria = null;
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal2 = Calendar.getInstance();
        cal2.add(Calendar.DATE, 0);
        Calendar cal1 = Calendar.getInstance();
        cal1.add(Calendar.DATE, (-1) * date);
        while (streamEventChunk.hasNext()) {
            String TweetChunk = "";
            double TrumpCount = 0, BernieCount = 0, ClintonCount = 0, TedCount = 0, MarcoCount = 0;
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            criteria = TwitterCriteria12.create().setUsername((String) variableExpressionURLName.execute(streamEvent))
                    .setSince(dateFormat.format(cal1.getTime())).setUntil(dateFormat.format(cal2.getTime()));
            for (Tweet12 t : TweetManager12.getTweets(criteria)) {
                TweetChunk = TweetChunk.concat(t.getText());
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
            for (String t : Marco) {
                MarcoCount = MarcoCount + StringUtils.countMatches(TweetChunk.toLowerCase(), t.toLowerCase());
            }
            double tot = TrumpCount + BernieCount + ClintonCount + TedCount + MarcoCount + 1;
            // System.out.println("Tot="+tot);
            List<Candidate2> list = new ArrayList<Candidate2>();
            list.add(new Candidate2("TRUMP", (double) ((TrumpCount / tot) * 100.00)));
            list.add(new Candidate2("BERNIE", (double) ((BernieCount / tot) * 100.00)));
            list.add(new Candidate2("CLINTON", (double) ((ClintonCount / tot) * 100.00)));
            list.add(new Candidate2("CRUZ", (double) ((TedCount / tot) * 100.00)));
            list.add(new Candidate2("RUBIE", (double) ((MarcoCount / tot) * 100.00)));
            Collections.sort(list, new Candidate2());
            StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
            if (tot != 1) {
                complexEventPopulater.populateComplexEvent(clonedEvent, new Object[] { list.get(0).getCandidateName(),
                        list.get(0).getCandidateRank() });
            } else {
                complexEventPopulater.populateComplexEvent(clonedEvent, new Object[] { "Other", 0 });
            }
            returnEventChunk.add(clonedEvent);

        }
        long lEndTime = System.currentTimeMillis();
        long difference = lEndTime - lStartTime;
        System.out.println("Get Party 2: " + difference);
        nextProcessor.process(returnEventChunk);
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
            date = (Integer)attributeExpressionExecutors[1].execute(null);
        }

        List<Attribute> attributeList = new ArrayList<Attribute>();
        // TRUMP,CLINTON,BERNIE,BEN,MALLEY,BUSH,CRUZ,CHRIS,FIORINA,GILMORE,GRAHAM,HUCKABEE,JOHN,GEORGE,RAND,RUBIE,RICK,WALKER
        attributeList.add(new Attribute("TopName", Attribute.Type.STRING));
        attributeList.add(new Attribute("Top", Attribute.Type.DOUBLE));

        return attributeList;

    }

}

class Candidate2 implements Comparator<Candidate2>, Comparable<Candidate2> {
    String name;
    double rank;

    Candidate2() {
    }

    Candidate2(String n, double a) {
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
    public int compareTo(Candidate2 d) {
        return (this.name).compareTo(d.name);
    }

    // Overriding the compare method to sort the age
    public int compare(Candidate2 d, Candidate2 d1) {
        return (int) (d1.rank - d.rank);
    }
}

class TwitterCriteria12 {
    private String username;
    private String since;
    private String until;
    private String querySearch;
    private int maxTweets;

    private TwitterCriteria12() {
    }

    public static TwitterCriteria12 create() {
        return new TwitterCriteria12();
    }

    public TwitterCriteria12 setUsername(String username) {
        this.username = username;
        return this;
    }

    public TwitterCriteria12 setSince(String since) {
        this.since = since;
        return this;
    }

    public TwitterCriteria12 setUntil(String until) {
        this.until = until;
        return this;
    }

    public TwitterCriteria12 setQuerySearch(String querySearch) {
        this.querySearch = querySearch;
        return this;
    }

    public TwitterCriteria12 setMaxTweets(int maxTweets) {
        this.maxTweets = maxTweets;
        return this;
    }

    String getUsername() {
        return username;
    }

    String getSince() {
        return since;
    }

    String getUntil() {
        return until;
    }

    String getQuerySearch() {
        return querySearch;
    }

    int getMaxTweets() {
        return maxTweets;
    }
}

class TweetManager12 {
    private static final org.slf4j.Logger logger =  LoggerFactory.getLogger(TweetManager12.class);
    private static final HttpClient defaultHttpClient = HttpClients.createDefault();

    static {
        Logger.getLogger("org.apache.http").setLevel(Level.OFF);
    }

    private static String getURLResponse(String username, String since, String until, String querySearch,
            String scrollCursor) throws Exception {

        String appendQuery = "";
        if (username != null) {
            appendQuery += "from:" + username;
        }
        if (since != null) {
            appendQuery += " since:" + since;
        }
        if (until != null) {
            appendQuery += " until:" + until;
        }
        if (querySearch != null) {
            appendQuery += " " + querySearch;
        }

        String url = String.format("https://twitter.com/i/search/timeline?f=realtime&q=%s&src=typd&max_position=%s",
                URLEncoder.encode(appendQuery, "UTF-8"), scrollCursor);
        // System.out.println(url);
        HttpGet httpGet = new HttpGet(url);
        HttpEntity resp = defaultHttpClient.execute(httpGet).getEntity();

        return EntityUtils.toString(resp);
    }

    public static List<Tweet12> getTweets(TwitterCriteria12 criteria) {
        List<Tweet12> results = new ArrayList<Tweet12>();

        try {
            String refreshCursor = null;
            outerLace: while (true) {
                JSONObject json = new JSONObject(getURLResponse(criteria.getUsername(), criteria.getSince(),
                        criteria.getUntil(), criteria.getQuerySearch(), refreshCursor));
                refreshCursor = json.getString("min_position");
                Document doc = Jsoup.parse((String) json.get("items_html"));
                Elements tweets = doc.select("div.js-stream-tweet");

                if (tweets.size() == 0) {
                    break;
                }

                for (Element tweet : tweets) {
                    String usernameTweet = tweet.select("span.username.js-action-profile-name b").text();
                    String txt = tweet.select("p.js-tweet-text").text().replaceAll("[^\\u0000-\\uFFFF]", "");
                    int retweets = Integer.valueOf(tweet
                            .select("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount")
                            .attr("data-tweet-stat-count").replaceAll(",", ""));
                    int favorites = Integer.valueOf(tweet
                            .select("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount")
                            .attr("data-tweet-stat-count").replaceAll(",", ""));
                    long dateMs = Long.valueOf(tweet.select("small.time span.js-short-timestamp").attr("data-time-ms"));
                    String id = tweet.attr("data-tweet-id");
                    String permalink = tweet.attr("data-permalink-path");

                    String geo = "";
                    Elements geoElement = tweet.select("span.Tweet-geo");
                    if (geoElement.size() > 0) {
                        geo = geoElement.attr("title");
                    }

                    Date date = new Date(dateMs);

                    Tweet12 t = new Tweet12();
                    t.setId(id);
                    t.setPermalink("https://twitter.com" + permalink);
                    t.setUsername(usernameTweet);
                    t.setText(txt);
                    t.setDate(date);
                    t.setRetweets(retweets);
                    t.setFavorites(favorites);
                    t.setMentions(processTerms("(@\\w*)", txt));
                    t.setHashtags(processTerms("(#\\w*)", txt));
                    t.setGeo(geo);

                    results.add(t);

                    if (criteria.getMaxTweets() > 0 && results.size() >= criteria.getMaxTweets()) {
                        break outerLace;
                    }
                }
            }
        } catch (Exception e) {
           // e.printStackTrace();
           logger.error(e.getMessage());
        }

        return results;
    }

    private static String processTerms(String patternS, String tweetText) {
        StringBuilder sb = new StringBuilder();
        Matcher matcher = Pattern.compile(patternS).matcher(tweetText);
        while (matcher.find()) {
            sb.append(matcher.group());
            sb.append(" ");
        }

        return sb.toString().trim();
    }

}

class Tweet12 {
    private String id;
    private String permalink;
    private String username;
    private String text;
    private Date date;
    private int retweets;
    private int favorites;
    private String mentions;
    private String hashtags;
    private String geo;

    public Tweet12() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPermalink() {
        return permalink;
    }

    public void setPermalink(String permalink) {
        this.permalink = permalink;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public int getRetweets() {
        return retweets;
    }

    public void setRetweets(int retweets) {
        this.retweets = retweets;
    }

    public int getFavorites() {
        return favorites;
    }

    public void setFavorites(int favorites) {
        this.favorites = favorites;
    }

    public String getMentions() {
        return mentions;
    }

    public void setMentions(String mentions) {
        this.mentions = mentions;
    }

    public String getHashtags() {
        return hashtags;
    }

    public void setHashtags(String hashtags) {
        this.hashtags = hashtags;
    }

    public String getGeo() {
        return geo;
    }

    public void setGeo(String geo) {
        this.geo = geo;
    }
}

