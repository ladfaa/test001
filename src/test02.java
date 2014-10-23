import mia.recommender.ch06.UserVectorToCooccurrenceMapper;
import mia.recommender.ch06.UserVectorToCooccurrenceReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

public class test02 {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "test02");
    job.setJarByClass(test02.class);
    job.setMapperClass(UserVectorToCooccurrenceMapper.class);
    job.setReducerClass(UserVectorToCooccurrenceReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VectorWritable.class);
    FileInputFormat.addInputPath(job, new Path("out1"));
    FileOutputFormat.setOutputPath(job, new Path("out2"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
