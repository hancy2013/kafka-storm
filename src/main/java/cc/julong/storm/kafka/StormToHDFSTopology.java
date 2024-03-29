package cc.julong.storm.kafka;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


/**
 * Storm实时计算集群从Kafka消息中间件中消费消息，有实时处理需求的可以走实时处理程序，
 * 还有需要进行离线分析的需求，如写入到HDFS进行分析。下面实现了一个Topology
 *
 * Created by zhangfeng on 2015/1/27.
 */
public class StormToHDFSTopology {

    public static class EventSpout extends BaseRichSpout {

        private static final Log LOG = LogFactory.getLog(EventSpout.class);
        private static final long serialVersionUID = 886149197481637894L;
        private SpoutOutputCollector collector;
        private Random rand;
        private String[] records;


        @Override
        public void open(Map conf, TopologyContext context,
                         SpoutOutputCollector collector) {
            this.collector = collector;
            rand = new Random();
            records = new String[] {
                    "10001     ef2da82d4c8b49c44199655dc14f39f6     4.2.1     HUAWEI G610-U00     HUAWEI     2     70:72:3c:73:8b:22     2014-10-13 12:36:35",
                    "10001     ffb52739a29348a67952e47c12da54ef     4.3     GT-I9300     samsung     2     50:CC:F8:E4:22:E2     2014-10-13 12:36:02",
                    "10001     ef2da82d4c8b49c44199655dc14f39f6     4.2.1     HUAWEI G610-U00     HUAWEI     2     70:72:3c:73:8b:22     2014-10-13 12:36:35"
            };
        }

        @Override
        public void nextTuple() {
            Utils.sleep(1000);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
            Date d = new Date(System.currentTimeMillis());
            String minute = df.format(d);
            String record = records[rand.nextInt(records.length)];
            LOG.info("EMIT[spout -> hdfs] " + minute + " : " + record);
            collector.emit(new Values(minute, record));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("minute", "record"));
        }
    }


    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter(" : ");
        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);
        // rotate files
        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimeUnit.MINUTES);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/storm/").withPrefix("app_").withExtension(".log");
        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://hadoop01:9000")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("event-spout", new EventSpout(), 3);
        builder.setBolt("hdfs-bolt", hdfsBolt, 2).fieldsGrouping("event-spout", new Fields("minute"));
        Config conf = new Config();
        String name = StormToHDFSTopology.class.getSimpleName();
        if (args != null && args.length > 0) {
            conf.put(Config.NIMBUS_HOST, args[0]);
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }


}