import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

public class SCColorMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {
    private int colorNum;

    private Random rand = new Random();

    public void configure(JobConf job) {
        colorNum = Integer.parseInt(job.get("sc.colornum"));

    }

    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        String[] line = value.toString().split("\\s");
        String colorBit = Integer.toString(SCUtils.power(2, rand
                .nextInt(colorNum)));

        // ///////////////
        // colorBit = Integer.toString(power(2,Integer.parseInt(line[0])));
        // ///////////////
        output.collect(new Text(line[0]), new Text(colorBit + ",1 " + line[1]));

    }
}
