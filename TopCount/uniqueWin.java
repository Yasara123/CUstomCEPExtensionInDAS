package org.wso2.siddhi.extension.TwitterSentiment;

import static java.util.concurrent.TimeUnit.HOURS;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.MetaComplexEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.parser.CollectionOperatorParser;
import org.wso2.siddhi.query.api.expression.Expression;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class uniqueWin extends WindowProcessor implements FindableProcessor{
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ConcurrentHashMap<String, StreamEvent> map ;
    private VariableExpressionExecutor[] variableExpressionExecutors;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        variableExpressionExecutors = new VariableExpressionExecutor[attributeExpressionExecutors.length];
        for (int i = 0; i < attributeExpressionExecutors.length; i++) {
            variableExpressionExecutors[i] =(VariableExpressionExecutor) attributeExpressionExecutors[i];
        }
    }

    @Override
    protected synchronized void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        ComplexEventChunk<StreamEvent> complexEventChunk = new ComplexEventChunk<StreamEvent>();

        StreamEvent streamEvent = streamEventChunk.getFirst();
        while (streamEvent != null) {
            StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
            clonedEvent.setType(StreamEvent.Type.EXPIRED);

            StreamEvent oldEvent = map.put(generateKey(clonedEvent), clonedEvent);
            if (oldEvent != null) {
                complexEventChunk.add(oldEvent);
            }
            StreamEvent next = streamEvent.getNext();
            streamEvent.setNext(null);
            complexEventChunk.add(streamEvent);
            streamEvent = next;
        }
        nextProcessor.process(complexEventChunk);
    }

    @Override
    public void start() {
        //Do nothing
        scheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                map = new ConcurrentHashMap<String, StreamEvent>();
                
            }
        }, 0, 24, HOURS);
    }

 
    @Override
    public void stop() {
        //Do nothing
        scheduler.shutdownNow();
    }


    @Override
    public Object[] currentState() {
        return new Object[]{map};
    }

    @Override
    public void restoreState(Object[] state) {
        map = (ConcurrentHashMap<String, StreamEvent>) state[0];
    }


    @Override
    public synchronized StreamEvent find(ComplexEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, map.values(),streamEventCloner);
    }

    @Override
    public Finder constructFinder(Expression expression, MetaComplexEvent metaComplexEvent, ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, EventTable> eventTableMap, int matchingStreamIndex, long withinTime) {
        return CollectionOperatorParser.parse(expression, metaComplexEvent, executionPlanContext, variableExpressionExecutors, eventTableMap, matchingStreamIndex, inputDefinition, withinTime);

    }

    private String generateKey(StreamEvent event) {
        StringBuilder stringBuilder = new StringBuilder();
        for (VariableExpressionExecutor executor : variableExpressionExecutors) {
            stringBuilder.append(event.getAttribute(executor.getPosition()));
        }
        return stringBuilder.toString();
    }

}
