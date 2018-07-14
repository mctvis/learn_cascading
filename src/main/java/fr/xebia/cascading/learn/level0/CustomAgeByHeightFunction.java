package fr.xebia.cascading.learn.level0;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CustomAgeByHeightFunction extends BaseOperation implements Function {

    public CustomAgeByHeightFunction() {
        super(/*Fields.ARGS*/new Fields("age","height","ageByHeight"));//since we will be replacing the fields in this case, we use the same fields as argumentselectors // put new Fields("age","height","ahgeByHeights")to know the error
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tupleEntry = functionCall.getArguments();
      //  System.out.println(tupleEntry);
        TupleEntry tuple = new TupleEntry(tupleEntry);
      //  System.out.println(tuple);

        int age = tuple.getInteger("age");
        double height = tuple.getDouble("height");

       // System.out.println(age + "|" + height);
        double ageByht = age / height;
        tuple.setString("ageByHeight", String.format("%.2f", ageByht));

        System.out.println(tuple);
        functionCall.getOutputCollector().add(tuple);
    }
}
