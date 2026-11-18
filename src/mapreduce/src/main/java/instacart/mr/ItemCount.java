package instacart.mr;

import java.io.IOException;
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

/** Pha một: đếm item phổ biến từ file giao dịch, mỗi dòng là một giỏ. */
public class ItemCount extends Configured implements Tool {
  public static class ItemMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
    private static final LongWritable ONE = new LongWritable(1);
    private final IntWritable item = new IntWritable();

    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
      String value = line.toString().trim();
      if (value.isEmpty()) return;
      for (String token : value.split("\\s+")) {
        item.set(Integer.parseInt(token));
        context.write(item, ONE);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) throw new IllegalArgumentException("Cách dùng: ItemCount <input> <output> <min_count>");
    getConf().setLong("instacart.min.count", Long.parseLong(args[2]));
    Job job = Job.getInstance(getConf(), "item-count");
    job.setJarByClass(ItemCount.class);
    job.setMapperClass(ItemMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(MinCountReducer.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
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
