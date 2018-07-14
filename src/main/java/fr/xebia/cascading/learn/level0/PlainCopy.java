package fr.xebia.cascading.learn.level0;

import cascading.flow.FlowDef;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.FirstBy;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Level0 is a very basic example of how to do a copy. It should provide you
 * with enough leads about where to look for more information. You will need to
 * complete later levels in order to make the tests pass.
 *
 * @see //http://docs.cascading.org/cascading/3.0/userguide/
 * @see //http://docs.cascading.org/cascading/3.0/javadoc/cascading-core/
 */
public class PlainCopy {

    /**
     * A copy is a job with an empty set of operations.
     * <p>
     * source field(s) : "line" sink field(s) : "line"
     */
    public static FlowDef createFlowDefUsing(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
        Pipe pipe = new Pipe("plainCopy");
        return FlowDef.flowDef()//
                .addSource(pipe, source) //
                .addTail(pipe)//
                .addSink(pipe, sink);
    }


    /**
     * source:name, age, sex, height
     * <p>
     * Remove tuples having unknown gender and ages below 10.
     * Now create a new field and place age/height data in it.
     * <p>
     * stage:name, age, sex, height, ageByHeight
     */
    public static FlowDef abx(Tap source, Tap sink) {
        Pipe pipe = new Pipe("inPipe");
        pipe = new Each(pipe, new Fields("age", "sex"), new CustomFilter());
        //System.out.println(pipe);
        pipe = new Each(pipe, new Insert(new Fields("ageByHeight"), 0),Fields.ALL);
        pipe = new Each(pipe, new Fields("age","height","ageByHeight"),new CustomAgeByHeightFunction(),Fields.REPLACE);
        return FlowDef.flowDef()//
                .addSource(pipe, source) //
                .addTail(pipe)//
                .addSink(pipe, sink);
    }
    //incoming tuples id , name , amount
    public static FlowDef calcAvg(Tap source, Tap sink){
        Pipe pipe= new Pipe("inPipe");
        pipe = new GroupBy(pipe,new Fields("id"));
    //incoming id,name,amount (grouped)
        pipe = new Every(pipe,new Fields("amount"),new CustomAverageBuffer(),Fields.ALL );//new CustomAverageBuffer returns a tuple with single field "average".
        pipe = new Discard(pipe,new Fields("name","amount"));
        return FlowDef.flowDef().addSource(pipe,source).addTailSink(pipe,sink);
    }
    public static FlowDef calcSumDiff(Tap source, Tap sink){
        Pipe pipe = new Pipe("inPipe");
        pipe=new Each(pipe,new Fields("amount","discount"),new CustomSumDiff(),Fields.ALL);//try All, SWAP , RESULTS AND REPLACE and understand why replace doesnt work.
        return FlowDef.flowDef().addSource(pipe,source).addTailSink(pipe,sink);
    }
}