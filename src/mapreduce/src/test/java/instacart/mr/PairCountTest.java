package instacart.mr;

import static org.junit.Assert.assertEquals;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PairCountTest {
  private MiniMRYarnCluster cluster;
  private Configuration conf;

  @Before
  public void startCluster() {
    cluster = new MiniMRYarnCluster("pair-count-test", 1);
    conf = new Configuration();
    cluster.init(conf);
    cluster.start();
    conf = cluster.getConfig();
    conf.set("fs.defaultFS", "file:///");
    conf.set("mapreduce.framework.name", "yarn");
  }

  @After
  public void stopCluster() {
    if (cluster != null) cluster.stop();
  }

  @Test
  public void countsPairsWithFrequentItemsOnly() throws Exception {
    FileSystem fs = FileSystem.get(conf);
    java.nio.file.Path localRoot = java.nio.file.Files.createTempDirectory("pair-count-test");
    Path input = new Path(localRoot.resolve("in").toUri());
    Path itemOutput = new Path(localRoot.resolve("items-out").toUri());
    Path pairOutput = new Path(localRoot.resolve("pairs-out").toUri());
    fs.mkdirs(input);
    try (var writer = new java.io.BufferedWriter(new java.io.OutputStreamWriter(
        fs.create(new HadoopPath(input, "part-00000")), StandardCharsets.UTF_8))) {
      writer.write("1 2 3\n1 2\n1 3\n2 3\n1 2 3\n4\n");
    }
    assertEquals(0, ToolRunner.run(conf, new ItemCount(), new String[]{input.toString(), itemOutput.toString(), "2"}));
    Path frequentItems = new Path(itemOutput, "part-r-00000");
    assertEquals(0, ToolRunner.run(conf, new PairCount(), new String[]{input.toString(), pairOutput.toString(), "2", frequentItems.toUri().toString()}));
    Map<Long, Long> actual = new HashMap<>();
    Path part = new Path(pairOutput, "part-r-00000");
    try (BufferedReader reader = new BufferedReader(new java.io.InputStreamReader(fs.open(part), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] values = line.split("\\t");
        actual.put(Long.parseLong(values[0]), Long.parseLong(values[1]));
      }
    }
    assertEquals(Map.of(pair(1, 2), 3L, pair(1, 3), 3L, pair(2, 3), 3L), actual);
  }

  private static long pair(int left, int right) {
    return ((long) left << 32) | (right & 0xffffffffL);
  }
}
