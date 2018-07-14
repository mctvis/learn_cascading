package fr.xebia.cascading.learn.level0;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CustomSumDiff extends BaseOperation implements Function {
    public CustomSumDiff(){
        super(2,new Fields("sum","difference"));
    }


    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tupleEntry=functionCall.getArguments();
        System.out.println(tupleEntry);
        int amount=tupleEntry.getInteger("amount");
        int discount=tupleEntry.getInteger("discount");
        int sum=amount+discount;
        int difference=amount-discount;
        Tuple tuple=new Tuple(sum,difference);
        functionCall.getOutputCollector().add(tuple);
    }
}
