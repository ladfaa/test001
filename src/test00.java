import mia.recommender.ch06.WikipediaToItemPrefsMapper;
import mia.recommender.ch06.WikipediaToUserVectorReducer;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;  
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
  


import java.io.IOException;  
import java.text.SimpleDateFormat;  
  
public class test00 {  
  
  
    static class FirstMapper extends Mapper<LongWritable, Text, Text, LongWritable> {  
  
        @Override  
        public void map(LongWritable key, Text value, Context context) {  
            String[] a = value.toString().split("\t");  
            String time = a[1];  
            try {  
                context.write(new Text(time), new LongWritable(1L));  
            } catch (IOException e) {  
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.  
            } catch (InterruptedException e) {  
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.  
            }  
        }  
    }  
  
    static class FirstReducer extends Reducer<Text, LongWritable, Text, LongWritable> {  
  
        @Override  
        public void reduce(Text key, Iterable<LongWritable> values, Context context) {  
            long a = 0;  
            for(LongWritable l : values){  
                a += l.get();  
            }  
            try {  
                context.write(new Text(key), new LongWritable(a));  
            } catch (IOException e) {  
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.  
            } catch (InterruptedException e) {  
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.  
            }  
        }  
    }  
  
    static class SecondMapper extends Mapper<LongWritable, Text, Text, LongWritable> {  
  
        @Override  
        public void map(LongWritable key, Text value, Context context) {  
            String[] a = value.toString().split("\t");  
            String time = a[0];  
            Long l = time == null? 0: Long.parseLong(time)%1000;  
            try {  
                context.write(new Text(l.toString()), new LongWritable(1L));  
            } catch (IOException e) {  
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.  
            } catch (InterruptedException e) {  
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.  
            }  
        }  
    }  
  
    static class SecondReducer extends Reducer<Text, LongWritable, Text, LongWritable> {  
  
        @Override  
        public void reduce(Text key, Iterable<LongWritable> values, Context context) {  
            long a = 0;  
            for(LongWritable l : values){  
                a += l.get();  
            }  
            try {  
                context.write(new Text(key), new LongWritable(a));  
            } catch (IOException e) {  
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.  
            } catch (InterruptedException e) {  
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.  
            }  
        }  
    }  
  
    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");  
  
    public static long stime, etime;  
  
    public static void main(String[] args) throws Exception {  
        Configuration configuration = new Configuration();  
        JobControl jobControl = new JobControl("test00");  
  
        Job job = new Job();  
        job.setJobName("first");  
        job.setJarByClass(test00.class);  
        FileInputFormat.addInputPath(job, new Path(args[0]));  
  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
        job.setMapperClass(WikipediaToItemPrefsMapper.class);  
        job.setReducerClass(WikipediaToUserVectorReducer.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(LongWritable.class);  
        job.setNumReduceTasks(8);//  
        ControlledJob controlledJob = new ControlledJob(configuration);  
        controlledJob.setJob(job);  
  
        Job job2 = new Job();  
        job2.setJobName("second");  
        job2.setJarByClass(test00.class);  
        FileInputFormat.addInputPath(job2, new Path(args[1]));  
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));  
        job2.setMapperClass(SecondMapper.class);  
        job2.setReducerClass(SecondReducer.class);  
        job2.setOutputKeyClass(Text.class);  
        job2.setOutputValueClass(LongWritable.class);  
        job.setNumReduceTasks(8);//  
        ControlledJob controlledJob2 = new ControlledJob(configuration);  
        controlledJob2.setJob(job2);  
  
        controlledJob2.addDependingJob(controlledJob);  
        jobControl.addJob(controlledJob);  
        jobControl.addJob(controlledJob2);  
        jobControl.run();  
    }  
  
}  