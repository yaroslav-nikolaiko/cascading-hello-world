package education;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.fluid.Fluid;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.Properties;

/**
 * Created by ynikolaiko on 5/5/16.
 */
public class WordCountFluid {
    public static void main(String[] args) {
        String inputPath =args[0];
        String outputPath =args[1];


        Scheme sourceScheme = new TextLine( new Fields( "line" ) );

        Tap source = new Hfs( sourceScheme, inputPath );

        Pipe assembly =  Fluid.assembly()
                .startBranch( "wordcount" )
                .each( new Fields( "line" ) )
                .function(
                        Fluid.function()
                                .RegexGenerator()
                                .fieldDeclaration( new Fields( "word" ) )
                                .patternString( "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)" ).end()
                )
                .outgoing( Fields.RESULTS )
                .groupBy( new Fields( "word" ) )
                .every( Fields.ALL )
                .aggregator(
                        Fluid.aggregator().Count( new Fields( "count" ) )
                )
                .outgoing( Fields.ALL )
                .completeGroupBy()
                .completeBranch();

        Scheme sinkScheme = new TextDelimited( new Fields( "word", "count" ) );
        Tap sink = new Hfs( sinkScheme, outputPath, SinkMode.REPLACE );

        Properties properties = AppProps.appProps()
                .setName( "word-count-application" )
                .setJarClass( WordCountFluid.class )
                .buildProperties();

        FlowConnector flowConnector = new HadoopFlowConnector( properties );
        Flow flow = flowConnector.connect( "word-count", source, sink, assembly );

        flow.complete();

    }
}
