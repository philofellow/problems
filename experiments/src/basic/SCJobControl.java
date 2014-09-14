import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.*;
import java.util.*;

public class SCJobControl {

    public JobControl SCJobs = new JobControl("SC");

    public Collection<Job> jobList = new ArrayList<Job>();

    private String tFile;

    private String colorNum;

    public SCJobControl(String treeFile) {
        tFile = treeFile;
    }

    public void SubmitJobs() {
        SCJobs.addJobs(jobList);
        SCJobs.run();
    }

    public JobConf CreateColorConf(String cur, String graph) {
        JobConf conf = new JobConf();
        conf.setJobName(cur);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(SCColorMapper.class);
        conf.setReducerClass(SCColorReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setJarByClass(SCColorMapper.class);
        FileInputFormat.setInputPaths(conf, new Path(graph));
        FileOutputFormat.setOutputPath(conf, new Path(cur));
        conf.set("sc.colornum", colorNum);
        conf.setNumReduceTasks(0);

        return conf;
    }

    public JobConf CreateFinalConf(String cur, String tpl, String tplSize,
            String isom) {
        JobConf conf = new JobConf();
        conf.setJobName(cur);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(SCFinalMapper.class);
        // conf.setCombinerClass(SCFinalCombiner.class);
        conf.setReducerClass(SCFinalReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setJarByClass(SCFinalMapper.class);
        FileInputFormat.setInputPaths(conf, new Path(tpl));
        FileOutputFormat.setOutputPath(conf, new Path(cur));
        conf.set("sc.template.size", tplSize);
        conf.set("sc.template.isom", isom);
        conf.set("sc.colornum", colorNum);

        return conf;
    }

    private JobConf CreateConf(String cur, String aChild, String pChild) {
        JobConf conf = new JobConf();
        conf.setJobName(cur);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(SCMapper.class);
        conf.setReducerClass(SCReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setJarByClass(SCMapper.class);
        FileInputFormat.setInputPaths(conf, new Path(aChild));
        if (!aChild.equals(pChild)) {
            FileInputFormat.addInputPath(conf, new Path(pChild));
        }
        FileOutputFormat.setOutputPath(conf, new Path(cur));
        conf.set("sc.activeChild", aChild);
        conf.set("sc.passiveChild", pChild);
        return conf;

    }

    public void Init() {
        try {
            String line;

            File f = new File(tFile);
            BufferedReader fReader = new BufferedReader(new FileReader(f));

            while ((line = fReader.readLine()) != null) {
                String[] st = line.split(" ");
                if (st[0].contains("final")) { // total count
                    Job job = new Job(CreateFinalConf(st[0], st[1], st[2],
                            st[3]));
                    job.setJobID(st[0]);
                    jobList.add(job);

                } else if (st[0].equals("i")) { // random coloring

                    colorNum = st[2];
                    Job job = new Job(CreateColorConf(st[0], st[1]));
                    job.setJobID(st[0]);
                    jobList.add(job);

                } else {
                    String activeChild = st[1];
                    String passiveChild = st[2];

                    Job job = new Job(CreateConf(st[0], activeChild,
                            passiveChild));
                    job.setJobID(st[0]);
                    jobList.add(job);
                }

            }
            fReader.close();
            fReader = new BufferedReader(new FileReader(f));
            while ((line = fReader.readLine()) != null) {
                String[] st = line.split(" ");
                Job curJob = findJobByID(st[0]);
                if (st[0].contains("final")) {
                    Job job = findJobByID(st[1]);
                    curJob.addDependingJob(job);
                } else if (st[0].equals("i")) {
                    continue;
                } else {
                    String activeChild = st[1];
                    String passiveChild = st[2];

                    Job job = findJobByID(activeChild);
                    curJob.addDependingJob(job);

                    job = findJobByID(passiveChild);
                    curJob.addDependingJob(job);

                }
            }
            fReader.close();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private Job findJobByID(String s) {
        for (Iterator<Job> iter = jobList.iterator(); iter.hasNext();) {
            Job job = iter.next();
            if (job.getJobID().equals(s)) {
                return job;
            }
        }
        return null;
    }
}
