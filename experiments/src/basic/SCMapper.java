import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

public class SCMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {

    private String activeChild;

    private String passiveChild;

    private String tID;

    private void Emit(String line, String flag,
            OutputCollector<Text, Text> output) throws IOException {
        String[] token = line.split("\\s");
        String vertex = token[0]; // vertex
        String colorCount = token[1]; // color,count,color,count,...
        String nei = token[2]; // neighbor,neighbor,...

        String value;

        if (flag.equals("active")) {
            value = colorCount + " 0 " + nei;
            output.collect(new Text(vertex), new Text(value));
        }

        if (flag.equals("passive")) {
            String[] neiToken = nei.split(",");
            for (int i = 0; i < neiToken.length; i++) {
                value = colorCount + " 1";
                output.collect(new Text(neiToken[i]), new Text(value));
            }

        }
    }

    public void configure(JobConf job) {
        activeChild = job.get("sc.activeChild");
        passiveChild = job.get("sc.passiveChild");
        String[] pathToken = job.get("map.input.file").split("/");
        tID = pathToken[pathToken.length - 2];

    }

    public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        // input: v[TAB]s_1,c_1,s_2,c_2,...,s_k,c_k[SPACE]u_1,u_2,u_3,...

        String line = value.toString();

        if (tID.equals(activeChild))
            Emit(line, "active", output);
        if (tID.equals(passiveChild))
            Emit(line, "passive", output);

    }
}
