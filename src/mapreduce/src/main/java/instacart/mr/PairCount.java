package instacart.mr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Pha hai: lọc item phổ biến rồi đếm cặp, mã hóa cặp thành một khóa long. */
public class PairCount extends Configured implements Tool {
  public static class PairMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    private static final LongWritable ONE = new LongWritable(1);
    private final LongWritable pair = new LongWritable();
    private final Set<Integer> frequent = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException {
      URI[] files = context.getCacheFiles();
      if (files == null || files.length != 1) throw new IOException("Cần đúng một file frequent items trong distributed cache");
      Path cached = new Path(files[0].getPath());
      FileSystem fs = cached.getFileSystem(context.getConfiguration());
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(cached), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] columns = line.split("\\t", 2);
          if (columns.length == 2) frequent.add(Integer.parseInt(columns[0]));
        }
      }
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
      String text = line.toString().trim();
      if (text.isEmpty()) return;
      String[] tokens = text.split("\\s+");
      int[] items = new int[tokens.length];
      int count = 0;
      for (String token : tokens) {
        int item = Integer.parseInt(token);
        if (frequent.contains(item)) items[count++] = item;
      }
      for (int left = 0; left < count; left++) {
        for (int right = left + 1; right < count; right++) {
          pair.set(((long) items[left] << 32) | (items[right] & 0xffffffffL));
          context.write(pair, ONE);
        }
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 4) throw new IllegalArgumentException("Cách dùng: PairCount <input> <output> <min_count> <items_file>");
    getConf().setLong("instacart.min.count", Long.parseLong(args[2]));
    Job job = Job.getInstance(getConf(), "pair-count");
    job.setJarByClass(PairCount.class);
    job.addCacheFile(new URI(args[3]));
    job.setMapperClass(PairMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(MinCountReducer.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new PairCount(), args));
  }
}
