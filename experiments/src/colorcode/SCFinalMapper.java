import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SCFinalMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        double count = 0.0;
        String[] colorCountPair = value.toString().split("\\s")[1].split(",");
        for (int i = 1; i < colorCountPair.length; i += 2) {
            count += Double.parseDouble(colorCountPair[i]);
        }

        output.collect(new Text("Count"), new Text(Double.toString(count)));

    }
}
