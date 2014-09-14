import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;

public class SCFinalCombiner extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        double sum = 0;
        while (values.hasNext()) {
            sum += Integer.parseInt(values.next().toString());
        }

        output.collect(key, new Text(Double.toString(sum)));
    }

}
