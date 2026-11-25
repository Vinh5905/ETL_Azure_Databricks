package instacart.mr;

import java.io.IOException;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Pha 1: đếm tần suất item và ghi những item đạt min_count vào một part file. */
public class ItemCount extends Configured implements Tool {
  public static class ItemMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
    private static final LongWritable ONE = new LongWritable(1L);
    private final IntWritable outputKey = new IntWritable();

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      String contents = line.toString().trim();
      if (contents.isEmpty()) {
        return;
      }
      TreeSet<Integer> uniqueItems = new TreeSet<>();
      for (String token : contents.split("\\s+")) {
        int item = Integer.parseInt(token);
        if (item < 0) {
          throw new IOException("idx phải không âm: " + item);
        }
        uniqueItems.add(item);
      }
      for (int item : uniqueItems) {
        outputKey.set(item);
        context.write(outputKey, ONE);
      }
    }
  }

  public static class ItemReducer extends MinCountReducer<IntWritable> {}

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      throw new IllegalArgumentException("Cần tham số: <input> <output> <min_count>");
    }
    long minCount = Long.parseLong(args[2]);
    if (minCount < 1) {
      throw new IllegalArgumentException("min_count phải >= 1");
    }

    getConf().setLong("instacart.min.count", minCount);
    Job job = Job.getInstance(getConf(), "item-count");
    job.setJarByClass(ItemCount.class);
    job.setMapperClass(ItemMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(ItemReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new ItemCount(), args));
  }
}
