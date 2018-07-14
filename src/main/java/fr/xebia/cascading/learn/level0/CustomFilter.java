package fr.xebia.cascading.learn.level0;

import cascading.flow.FlowProcess;
import cascading.operation.*;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CustomFilter extends BaseOperation implements Filter /*Function*/ {
    public CustomFilter() {
        super(2);
    }


    public boolean isRemove(FlowProcess flowProcess, FilterCall call) {
        TupleEntry arguments = call.getArguments();
       // System.out.println(arguments);
        int age = arguments.getInteger("age");
        //System.out.println(age);
        String sex = arguments.getString("sex");
        return age < 10 || !("m".equalsIgnoreCase(sex) || "f".equalsIgnoreCase(sex));
    }

/*   @Override
   public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
       TupleEntry tupleEntry = functionCall.getArguments();
     //  System.out.println(tupleEntry);
       TupleEntry tuple = new TupleEntry(tupleEntry);
      // System.out.println(tupleEntry);
       int age = tuple.getInteger("age");
       String sex = tuple.getString("sex");

       boolean x = age < 10 || !("m".equalsIgnoreCase(sex) || "f".equalsIgnoreCase(sex));
       //System.out.println(age+"|"+sex+"|"+x);
       if(!x){
           System.out.println(tuple);
           functionCall.getOutputCollector().add(tuple);
       }

   }*/

}