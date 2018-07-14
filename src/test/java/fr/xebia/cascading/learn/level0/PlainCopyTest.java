package fr.xebia.cascading.learn.level0;

import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import fr.xebia.cascading.learn.Assert;
import org.junit.Before;
import org.junit.Test;

public class PlainCopyTest {

    @Before
    public void doNotCareAboutOsStuff() {
        System.setProperty("line.separator", "\n");
    }

    @Test
    public void knowTheBasics() throws Exception {
        // input of the job
        String sourcePath = "src/test/resources/hadoop-wiki-sample.txt";
        Tap<?, ?, ?> source = new FileTap(new TextLine(new Fields("line")), sourcePath);

        // actual output of the job
        String sinkPath = "target/level0/plain-copy.txt";
        Tap<?, ?, ?> sink = new FileTap(new TextLine(), sinkPath, SinkMode.REPLACE);

        // create the job definition, and run it
        FlowDef flowDef = PlainCopy.createFlowDefUsing(source, sink);
        new LocalFlowConnector().connect(flowDef).complete();

        // check that actual and expect outputs are the same
        Assert.sameContent(sinkPath, "src/test/resources/level0/expectation.txt");
    }

    @Test
    public void abxTest() throws Exception {
        String sourcePath = "src/test/resources/level0/custom_file.txt";
        Tap source = new FileTap(new TextDelimited(new Fields("name", "age", "sex", "height"), true, "|"), sourcePath);
        // actual output of the job
        String sinkPath = "target/level0/paaa.txt";
        Tap sink = new FileTap(new TextDelimited(new Fields("name", "age", "sex", "height","ageByHeight"), ";"), sinkPath, SinkMode.REPLACE);//pachi operation garisakepachi pipe bata niskida thyakai yei fields haru hunu paryo natra error aucha

        // create the job definition, and run it
        FlowDef flowDef = PlainCopy.abx(source, sink);
        new LocalFlowConnector().connect(flowDef).complete();

        // check that actual and expect outputs are the same
        Assert.sameContent(sinkPath, "src/test/resources/level0/custom_expect.txt");
    }
    @Test
    public void averageTest(){
        String sourcePath="src/test/resources/level0/custom_average.txt";
        Tap source= new FileTap(new TextDelimited(new Fields("id", "name", "amount"),true,"|"),sourcePath);

        String sinkPath="target/level0/avgoutput.txt";
        Tap sink= new FileTap(new TextDelimited(true,"|"),sinkPath,SinkMode.REPLACE);

        FlowDef flowDef = PlainCopy.calcAvg(source, sink);
        new LocalFlowConnector().connect(flowDef).complete();
    }
    @Test
    public void sumDiff(){
        String sourcePath="src/test/resources/level0/custom_sumdiff.txt";
        Tap source= new FileTap(new TextDelimited(new Fields("id", "name", "amount","discount"),true,"|"),sourcePath);

        String sinkPath="target/level0/sumAndDiff.txt";
        Tap sink= new FileTap(new TextDelimited(true,"|"),sinkPath,SinkMode.REPLACE);

        FlowDef flowDef = PlainCopy.calcSumDiff(source, sink);
        new LocalFlowConnector().connect(flowDef).complete();
    }

}
