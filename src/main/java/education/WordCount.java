package education;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.Properties;

/**
 * Created by yaroslav on 4/9/16.
 */
public class WordCount {
    public static void main( String[] args )
    {
        String docPath = args[ 0 ];
        String wcPath = args[ 1 ];

        //String currentDir = System.getProperty("user.dir") + "/";
        //docPath=currentDir+docPath;
        //wcPath=currentDir+wcPath;

        Properties properties = new Properties();
        AppProps.setApplicationJarClass( properties, WordCount.class );
        HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

        // create source and sink taps
        Tap docTap = new Hfs( new TextDelimited(true, "\t"), docPath );
        Tap wcTap =  new Hfs( new TextDelimited(true, "\t"), wcPath );

        // specify a regex operation to split the "document" text lines into a token stream
        Fields token = new Fields( "token" );
        Fields text = new Fields( "text" );
        RegexSplitGenerator splitter = new RegexSplitGenerator( token, "[ \\[\\]\\(\\),.]" );
        // only returns "token"
        Pipe docPipe = new Each( "token", text, splitter, Fields.RESULTS );

        // determine the word counts
        Pipe wcPipe = new Pipe( "wc", docPipe );
        wcPipe = new GroupBy( wcPipe, token );
        wcPipe = new Every( wcPipe, Fields.ALL, new Count(), Fields.ALL );

        // connect the taps, pipes, etc., into a flow
        FlowDef flowDef = FlowDef.flowDef()
                .setName( "wc" )
                .addSource( docPipe, docTap )
                .addTailSink( wcPipe, wcTap );

        // write a DOT file and run the flow
        Flow wcFlow = flowConnector.connect( flowDef );
        wcFlow.writeDOT( "/tmp/wc/output/dot/wc.dot" );
        wcFlow.complete();
    }
}
