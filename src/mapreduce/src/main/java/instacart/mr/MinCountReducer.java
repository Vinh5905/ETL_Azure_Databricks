package instacart.mr;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/** Cộng số đếm và chỉ ghi khóa có tần suất đạt ngưỡng tối thiểu. */
public class MinCountReducer<K> extends Reducer<K, LongWritable, K, LongWritable> {
  private long minCount;
  private final LongWritable output = new LongWritable();

  @Override
  protected void setup(Context context) {
    minCount = context.getConfiguration().getLong("instacart.min.count", 1L);
  }

  @Override
  protected void reduce(K key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    long sum = 0;
    for (LongWritable value : values) {
      sum += value.get();
    }
    if (sum >= minCount) {
      output.set(sum);
      context.write(key, output);
    }
  }
}
