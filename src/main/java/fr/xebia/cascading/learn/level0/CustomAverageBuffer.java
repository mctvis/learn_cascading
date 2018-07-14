package fr.xebia.cascading.learn.level0;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

public class CustomAverageBuffer extends BaseOperation implements Buffer{
    public CustomAverageBuffer(){
        super(1,new Fields("average"));
    }
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        long count=0;
        long sum=0;
        Iterator<TupleEntry> arguments= bufferCall.getArgumentsIterator();
        System.out.println(arguments);

        while( arguments.hasNext() )
        {
            count++;
            sum += arguments.next().getInteger( "amount");
        }
        Tuple tuple=new Tuple(sum/count);//the single value is mapped to "average" field implicitly
        System.out.println(tuple);

        bufferCall.getOutputCollector().add(tuple);

    }
}
