package org.wso2.siddhi.extension.TwitterSentiment;

import static java.util.concurrent.TimeUnit.HOURS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.json.JSONObject;
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
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;

import com.clearspring.analytics.stream.ITopK;
import com.clearspring.analytics.stream.ScoredItem;
/*
#TweetReader:getTop2(4,10,Fparty, 990587,87261,829347,209493)
*/
public class CountTop2 extends StreamProcessor {
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private boolean first=true;
    private int PassToOut=4;
    private int MaxLength=10;
    private int T=0;
    private int Cl=0;
    private int Cr=0;
    private int B=0;    
    int peek = 4;
    VariableExpressionExecutor VaribleExecutorText;
    CustomConcurrentStreamSummary13 tpK;
    CustomConcurrentStreamSummary13 tpK2;
    private static final Logger logger = LoggerFactory.getLogger(CountTop2.class);
    @Override
    public void start() {
        // TODO Auto-generated method stub
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {          
                tpK = new CustomConcurrentStreamSummary13<String>(MaxLength);
                if (logger.isInfoEnabled()) {
                    logger.info("Created new window 1 in top count... ");
                }
                if (first) {
                tpK.offer("TRUMP", T);
                T=0;
                tpK.offer("CRUZ", Cr);
                Cr=0;
                tpK.offer("BERNIE", B);
                B=0;
                tpK.offer("CLINTON", Cl);
                Cl=0;
                first=false;
                }
                if (tpK2 != null) {
                    ArrayList<String> newList;
                    List stuff = (List) tpK2.peekWithScores((int) tpK2.size());
                    String Processed = "";
                    for (int i = 0; i < (int) tpK2.size(); i++) {
                        JSONObject jsonObj = new JSONObject(stuff.get(i));
                        String Val = jsonObj.getString("item");
                        int count = jsonObj.getInt("count");
                        tpK.offer(Val, count);
                    }
               
                }
            }
        }, 0, 16, HOURS);
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                tpK2 = new CustomConcurrentStreamSummary13<String>(MaxLength);     
                if (logger.isInfoEnabled()) {
                    logger.info("Created new window 2 in top count... ");
                }
            }
        }, 8, 16, HOURS);
        
    }

    @Override
    public void stop() {
        scheduler.shutdownNow();
        first=true;
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
        String rawString;
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>();
        StreamEvent streamEvent;
        while (streamEventChunk.hasNext()) {
            streamEvent = streamEventChunk.next();
            streamEventChunk.remove();
            rawString = (String) VaribleExecutorText.execute(streamEvent);
            tpK.offer(rawString.toUpperCase());   
            if(tpK2!=null){
                tpK2.offer(rawString.toUpperCase());
            }
            ArrayList<String> newList;            
            peek = (int) ((tpK.size() < PassToOut) ? tpK.size() : PassToOut);
            List stuff = (List) tpK.peekWithScores(peek);
            int TRUMP=0,CLOINTON=0,CRUZ=0,BERNIE=0;
            for (int i = 0; i < peek; i++) {
                JSONObject jsonObj = new JSONObject(stuff.get(i));
                String Val = jsonObj.getString("item");
                int count = jsonObj.getInt("count");
                if (Val.contains("TRUMP"))
                    TRUMP=count;
                if (Val.contains("CLINTON"))
                    CLOINTON=count; 
                if (Val.contains("BERNIE"))
                    BERNIE=count; 
                if (Val.contains("CRUZ"))
                    CRUZ=count;                 
            }
            complexEventPopulater.populateComplexEvent(streamEvent, new Object[] {TRUMP,CRUZ,CLOINTON,BERNIE});
            returnEventChunk.add(streamEvent);           
        }   
    nextProcessor.process(returnEventChunk);

    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // TODO Auto-generated method stub
        if (!(attributeExpressionExecutors.length == 7)) {
            throw new UnsupportedOperationException("Invalid number of Arguments");
        }
        if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
            PassToOut = ((Integer) attributeExpressionExecutors[0].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            MaxLength = ((Integer) attributeExpressionExecutors[1].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[2] instanceof VariableExpressionExecutor) {
            VaribleExecutorText = (VariableExpressionExecutor) attributeExpressionExecutors[2];
        } else {
            throw new UnsupportedOperationException("The first parameter should be Tweet Text");
        }
        if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
            T = ((Integer) attributeExpressionExecutors[3].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[4] instanceof ConstantExpressionExecutor) {
            Cr = ((Integer) attributeExpressionExecutors[4].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[5] instanceof ConstantExpressionExecutor) {
            Cl = ((Integer) attributeExpressionExecutors[5].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        if (attributeExpressionExecutors[6] instanceof ConstantExpressionExecutor) {
            B = ((Integer) attributeExpressionExecutors[6].execute(null));
        } else {
            throw new UnsupportedOperationException("The first parameter should be an integer");
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();    
        attributeList.add(new Attribute("Tcount", Attribute.Type.INT));       
        attributeList.add(new Attribute("Crcount", Attribute.Type.INT));
        attributeList.add(new Attribute("Ccount", Attribute.Type.INT));  
        attributeList.add(new Attribute("Bcount", Attribute.Type.INT));
        return attributeList;
    }
}

class CustomConcurrentStreamSummary13<T> implements ITopK<T> {
    private final int capacity;
    private final ConcurrentHashMap<T, ScoredItem<T>> itemMap;
    private final AtomicReference<ScoredItem<T>> minVal;
    private final AtomicLong size;
    private final AtomicBoolean reachCapacity;

    public CustomConcurrentStreamSummary13(final int capacity) {
        this.capacity = capacity;
        this.minVal = new AtomicReference<ScoredItem<T>>();
        this.size = new AtomicLong(0);
        this.itemMap = new ConcurrentHashMap<T, ScoredItem<T>>(capacity);
        this.reachCapacity = new AtomicBoolean(false);
    }
    @Override
    public boolean offer(final T element) {
        return offer(element, 1);
    }
    @Override
    public boolean offer(final T element, final int incrementCount) {
        long val = incrementCount;
        ScoredItem<T> value = new ScoredItem<T>(element, incrementCount);
        ScoredItem<T> oldVal = itemMap.putIfAbsent(element, value);
        if (oldVal != null) {
            val = oldVal.addAndGetCount(incrementCount);
        } else if (reachCapacity.get() || size.incrementAndGet() > capacity) {
            reachCapacity.set(true);

            ScoredItem<T> oldMinVal = minVal.getAndSet(value);
            itemMap.remove(oldMinVal.getItem());

            while (oldMinVal.isNewItem()) {
                // Wait for the oldMinVal so its error and value are completely up to date.
                // no thread.sleep here due to the overhead of calling it - the waiting time will be microseconds.
            }
            long count = oldMinVal.getCount();

            value.addAndGetCount(count);
            value.setError(count);
        }
        value.setNewItem(false);
        minVal.set(getMinValue());

        return val != incrementCount;
    }

    private ScoredItem<T> getMinValue() {
        ScoredItem<T> minVal = null;
        for (ScoredItem<T> entry : itemMap.values()) {
            if (minVal == null || (!entry.isNewItem() && entry.getCount() < minVal.getCount())) {
                minVal = entry;
            }
        }
        return minVal;
    }

    public long size() {
        return size.get();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (ScoredItem entry : itemMap.values()) {
            sb.append("(" + entry.getCount() + ": " + entry.getItem() + ", e: " + entry.getError() + "),");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public List<T> peek(final int k) {
        List<T> toReturn = new ArrayList<T>(k);
        List<ScoredItem<T>> values = peekWithScores(k);
        for (ScoredItem<T> value : values) {
            toReturn.add(value.getItem());
        }
        return toReturn;
    }

    public List<ScoredItem<T>> peekWithScores(final int k) {
        List<ScoredItem<T>> values = new ArrayList<ScoredItem<T>>();
        for (Map.Entry<T, ScoredItem<T>> entry : itemMap.entrySet()) {
            ScoredItem<T> value = entry.getValue();
            values.add(new ScoredItem<T>(value.getItem(), value.getCount(), value.getError()));
        }
        Collections.sort(values);
        values = values.size() > k ? values.subList(0, k) : values;
        return values;
    }

}
