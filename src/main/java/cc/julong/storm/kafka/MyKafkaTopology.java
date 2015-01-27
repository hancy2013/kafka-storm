package cc.julong.storm.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


/**
 * Storm Topology程序，主要是完成wordcount
 * 从Kafka读取订阅的消息行，通过空格拆分出单个单词，然后再做词频统计计算
 *
 * Created by zhangfeng on 2015/1/26.
 */
public class MyKafkaTopology {

    public static class KafkaWordSplitter extends BaseRichBolt {

        private static final Log LOG = LogFactory.getLog(KafkaWordSplitter.class);
        private static final long serialVersionUID = 886149197481637894L;
        private OutputCollector collector;


        @Override
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
        }


        @Override
        public void execute(Tuple input) {
            String line = input.getString(0);
            LOG.info("RECV[kafka -> splitter] " + line);
            String[] words = line.split("\\s+");
            for(String word : words) {
                LOG.info("EMIT[splitter -> counter] " + word);
                collector.emit(input, new Values(word, 1));
            }
            collector.ack(input);
        }


        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }


    public static class WordCounter extends BaseRichBolt {

        private static final Log LOG = LogFactory.getLog(WordCounter.class);
        private static final long serialVersionUID = 886149197481637894L;
        private OutputCollector collector;
        private Map<String, AtomicInteger> counterMap;


        @Override
        public void prepare(Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
            this.counterMap = new HashMap<String, AtomicInteger>();
        }


        @Override
        public void execute(Tuple input) {
            String word = input.getString(0);
            int count = input.getInteger(1);
            LOG.info("RECV[splitter -> counter] " + word + " : " + count);
            AtomicInteger ai = this.counterMap.get(word);
            if(ai == null) {
                ai = new AtomicInteger();
                this.counterMap.put(word, ai);
            }
            ai.addAndGet(count);
            collector.ack(input);
            LOG.info("CHECK statistics map: " + this.counterMap);
        }


        @Override
        public void cleanup() {
            LOG.info("The final result:");
            Iterator<Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
            while(iter.hasNext()) {
                Entry<String, AtomicInteger> entry = iter.next();
                LOG.info(entry.getKey() + "\t:\t" + entry.getValue().get());
            }
        }


        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }


    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        String zks = "hadoop02:2181,hadoop03:2181,hadoop04:2181/kafka";
        String topic = "test";
        //zookeeper上用于存储当前处理到哪个Offset了,默认是保存到zookeeper的根目录下，推荐自己创建一个节点存放
        String zkRoot = "/kafka";
        //Kafka中当前处理到哪的Offset是由客户端自己管理的。所以，后面两个的目的，其实是在zookeeper上建立一个 $zk_root/$spout_id 的节点，其值是一个map，存放了当前Spout处理的Offset的信息。
        String id = "word";
        //一个KafkaSpout只能去处理一个topic的内容，所以，它要求初始化时提供如下与topic相关信息：
        //Kafka集群中的Broker地址 （IP+Port）
        //通过Zookeeper获取Kafka集群中的Broker的地址
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        //告诉KafkaSpout如何去解码数据，生成Storm内部传递数据
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        //该配置是指，如果该Topology因故障停止处理，下次正常运行时是否从Spout对应数据源Kafka中的该订阅Topic的起始位置开始读取，如果forceFromStart=true，
        // 则之前处理过的Tuple还要重新处理一遍，否则会从上次处理的位置继续处理，保证Kafka中的Topic数据不被重复处理，是在数据源的位置进行状态记录
        spoutConf.forceFromStart = false;
        //KafkaSpout初始化时，会去取spoutConfig.zkServers 和 spoutConfig.zkPort 变量的值，而该值默认是没塞的，所以是空，那么它就会去取当前运行的Storm所配置的zookeeper地址和端口，
        // 而本地运行的Storm，是一个临时的zookeeper实例，并不会真正持久化。所以，每次关闭后，数据就没了。
        // 本地模式，要显示的去配置
        spoutConf.zkServers = Arrays.asList(new String[] {"hadoop02", "hadoop03", "hadoop04"});
        spoutConf.zkPort = 2181;

        //构建Storm的Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 5); // Kafka我们创建了一个5分区的Topic，这里并行度设置为5
        builder.setBolt("word-splitter", new KafkaWordSplitter(), 2).shuffleGrouping("kafka-reader");
        builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-splitter", new Fields("word"));

        Config conf = new Config();

        String name = MyKafkaTopology.class.getSimpleName();
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
