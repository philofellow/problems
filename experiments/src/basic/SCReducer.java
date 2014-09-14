import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.Text;

public class SCReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> {

    private int [] activeColorCountPair = new int [0]; // [2i] and [2i+1] are
                                                        // (color,count)

    // pair

    private ArrayList<int[]> passiveColorCountPair;

    private String nei;

    private Map<Integer, Integer> colorCountMap;

    private void ParseValue(Iterator<Text> values) {
        passiveColorCountPair = new ArrayList<int[]>();
        while (values.hasNext()) {
            String[] value = values.next().toString().split("\\s");
            String[] colorCount = value[0].split(",");
            int [] tmpPair = new int [colorCount.length];
            for (int i = 0; i < colorCount.length; i++) {
                tmpPair[i] = Integer.parseInt(colorCount[i]);
            }

            if (value[1].equals("0")) { // active value
                activeColorCountPair = tmpPair;
                nei = value[2];

            } else { // passive value
                passiveColorCountPair.add(tmpPair);
            }
        }
    }

    private boolean Aggregate() {
        colorCountMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < activeColorCountPair.length; i += 2) {
            int activeColor = activeColorCountPair[i];
            int activeCount = activeColorCountPair[i + 1];
            for (int [] passivePair : passiveColorCountPair) {
                for (int j = 0; j < passivePair.length; j += 2) {
                    int passiveColor = passivePair[j];
                    int passiveCount = passivePair[j + 1];
                    if ((activeColor & passiveColor) == 0)
                    // color set intersection is empty
                    {
                        int newColor = activeColor | passiveColor;
                        if (!colorCountMap.containsKey(newColor)) {
                            colorCountMap.put(new Integer(newColor),
                                    new Integer(activeCount * passiveCount));
                        } else {
                            int count = colorCountMap.get(newColor);
                            count += activeCount * passiveCount;
                            colorCountMap.put(new Integer(newColor),
                                    new Integer(count));
                        }
                    }
                }
            }
        }
        return !colorCountMap.isEmpty();
    }

    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        ParseValue(values);
        if (Aggregate()) {
            String outputValue = new String();
            for (Object color : colorCountMap.keySet()) {
                Integer count = colorCountMap.get(color);
                outputValue += (color.toString() + "," + count.toString() + ",");
            }
            outputValue += (" " + nei);

            output.collect(key, new Text(outputValue));
        }

    }

}
