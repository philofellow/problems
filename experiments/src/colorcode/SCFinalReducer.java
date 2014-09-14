import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;

public class SCFinalReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {
    private int isom;

    private int colorNum;

    private int tplSize;

    public void configure(JobConf job) {
        isom = Integer.parseInt(job.get("sc.template.isom"));
        colorNum = Integer.parseInt(job.get("sc.colornum"));
        tplSize = Integer.parseInt(job.get("sc.template.size"));

    }

    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        double sum = 0.0;
        while (values.hasNext()) {
            sum += Double.parseDouble(values.next().toString());
        }

        sum /= isom;
        sum /= SCUtils.Prob(colorNum, tplSize);

        output.collect(key, new Text(Double.toString(sum)));
    }

}
