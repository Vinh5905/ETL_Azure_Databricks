package instacart.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configured;
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

/** Pha 2: lọc item phổ biến từ distributed cache, đếm cặp và mã hóa cặp thành long. */
public class PairCount extends Configured implements Tool {
  public static class PairMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    private static final LongWritable ONE = new LongWritable(1L);
    private final LongWritable outputKey = new LongWritable();
    private final Set<Integer> frequentItems = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException {
      try (BufferedReader reader = new BufferedReader(new FileReader("frequent_items"))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split("\\t", 2);
          frequentItems.add(Integer.parseInt(fields[0]));
        }
      }
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      String contents = line.toString().trim();
      if (contents.isEmpty()) {
        return;
      }
      TreeSet<Integer> selectedItems = new TreeSet<>();
      for (String token : contents.split("\\s+")) {
        int item = Integer.parseInt(token);
        if (item < 0) {
          throw new IOException("idx phải không âm: " + item);
        }
        if (frequentItems.contains(item)) {
          selectedItems.add(item);
        }
      }

      List<Integer> items = new ArrayList<>(selectedItems);
      for (int left = 0; left < items.size(); left++) {
        for (int right = left + 1; right < items.size(); right++) {
          long packedPair = ((long) items.get(left) << 32) | (items.get(right) & 0xffffffffL);
          outputKey.set(packedPair);
          context.write(outputKey, ONE);
        }
      }
    }
  }

  public static class PairReducer extends MinCountReducer<LongWritable> {}

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 4) {
      throw new IllegalArgumentException("Cần tham số: <input> <output> <min_count> <items_file>");
    }
    long minCount = Long.parseLong(args[2]);
    if (minCount < 1) {
      throw new IllegalArgumentException("min_count phải >= 1");
    }
    URI cacheFile = new URI(args[3] + "#frequent_items");

    getConf().setLong("instacart.min.count", minCount);
    Job job = Job.getInstance(getConf(), "pair-count");
    job.setJarByClass(PairCount.class);
    job.addCacheFile(cacheFile);
    job.createSymlink();
    job.setMapperClass(PairMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(PairReducer.class);
    job.setNumReduceTasks(getConf().getInt("mapreduce.job.reduces", 1));
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
