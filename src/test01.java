import java.io.IOException;
import java.util.StringTokenizer;

import mia.recommender.ch06.WikipediaToItemPrefsMapper;
import mia.recommender.ch06.WikipediaToUserVectorReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;

public class test01 {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "test01");
    job.setJarByClass(test01.class);
    job.setMapperClass(WikipediaToItemPrefsMapper.class);
    job.setReducerClass(WikipediaToUserVectorReducer.class);
    job.setMapOutputKeyClass(VarLongWritable.class);
    job.setMapOutputValueClass(VarLongWritable.class);
    job.setOutputKeyClass(VarLongWritable.class);
    job.setOutputValueClass(VectorWritable.class);
    FileInputFormat.addInputPath(job, new Path("in"));
    FileOutputFormat.setOutputPath(job, new Path("out1"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
