package instacart.mr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Kiểm tra hai pha Apriori chạy thật qua MiniMRYarnCluster trên 5 giỏ đồ chơi. */
public class PairCountTest {
  private static MiniMRYarnCluster cluster;
  private static Path testDir;
  private static Configuration clusterConf;

  @BeforeClass
  public static void startCluster() throws Exception {
    testDir = Files.createTempDirectory("instacart-minimr-");
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    conf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
    conf.setInt("yarn.nodemanager.resource.memory-mb", 2048);
    conf.setInt("yarn.scheduler.minimum-allocation-mb", 256);
    conf.setInt("yarn.scheduler.maximum-allocation-mb", 2048);
    conf.setInt("mapreduce.map.memory.mb", 512);
    conf.setInt("mapreduce.reduce.memory.mb", 512);
    conf.setBoolean("yarn.nodemanager.pmem-check-enabled", false);
    conf.setBoolean("yarn.nodemanager.vmem-check-enabled", false);
    cluster = new MiniMRYarnCluster("instacart-pair-count", 1);
    cluster.init(conf);
    cluster.start();
    clusterConf = new Configuration(cluster.getConfig());
    clusterConf.set("fs.defaultFS", "file:///");
    clusterConf.set("mapreduce.framework.name", "yarn");
    clusterConf.setInt("mapreduce.job.reduces", 1);
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    if (cluster != null) {
      cluster.stop();
    }
  }

  @Test
  public void countsOnlyFrequentPairsFromToyTransactions() throws Exception {
    Path inputDir = testDir.resolve("input");
    Path itemOutput = testDir.resolve("items");
    Path pairOutput = testDir.resolve("pairs");
    Files.createDirectories(inputDir);
    Files.write(
        inputDir.resolve("transactions.txt"),
        List.of("0 1 2 9", "0 1 2 9", "0 1", "0 2", "1 2"),
        StandardCharsets.UTF_8);

    int itemStatus =
        ToolRunner.run(
            new Configuration(clusterConf),
            new ItemCount(),
            new String[] {inputDir.toUri().toString(), itemOutput.toUri().toString(), "3"});
    assertEquals(0, itemStatus);
    assertEquals(List.of("0\t4", "1\t4", "2\t4"), readPartFile(itemOutput));

    int pairStatus =
        ToolRunner.run(
            new Configuration(clusterConf),
            new PairCount(),
            new String[] {
              inputDir.toUri().toString(),
              pairOutput.toUri().toString(),
              "3",
              itemOutput.resolve("part-r-00000").toUri().toString()
            });
    assertEquals(0, pairStatus);

    Map<Long, Long> actual = readPackedPairs(pairOutput);
    Map<Long, Long> expected = new HashMap<>();
    expected.put(pack(0, 1), 3L);
    expected.put(pack(0, 2), 3L);
    expected.put(pack(1, 2), 3L);
    assertEquals(expected, actual);
    assertTrue("item 9 không đạt ngưỡng nên không sinh cặp", !actual.containsKey(pack(0, 9)));
  }

  private static List<String> readPartFile(Path outputDir) throws IOException {
    return Files.readAllLines(outputDir.resolve("part-r-00000"), StandardCharsets.UTF_8);
  }

  private static Map<Long, Long> readPackedPairs(Path outputDir) throws IOException {
    Map<Long, Long> pairs = new HashMap<>();
    for (String line : readPartFile(outputDir)) {
      String[] fields = line.split("\\t");
      pairs.put(Long.parseLong(fields[0]), Long.parseLong(fields[1]));
    }
    return pairs;
  }

  private static long pack(int left, int right) {
    return ((long) left << 32) | (right & 0xffffffffL);
  }
}
