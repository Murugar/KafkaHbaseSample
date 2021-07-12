package com.iqmsoft;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class WeatherTopology {

	static class FilterAirportsBolt extends BaseBasicBolt {
		Pattern stationPattern;
		Pattern weatherPattern;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			stationPattern = Pattern.compile("<station_id>K([A-Z]{3})</station_id>");
			weatherPattern = Pattern.compile("<weather>([^<]*)</weather>");
			super.prepare(stormConf, context);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String report = tuple.getString(0);
			Matcher stationMatcher = stationPattern.matcher(report);
			if(!stationMatcher.find()) {
				return;
			}
			Matcher weatherMatcher = weatherPattern.matcher(report);
			if(!weatherMatcher.find()) {
				return;
			}
			collector.emit(new Values(stationMatcher.group(1), weatherMatcher.group(1)));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("airport", "weather"));
		}

	}

	static class ExtractWeatherBolt extends BaseBasicBolt {

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String weather = input.getStringByField("weather");
			boolean fog = weather.contains("fog");
			boolean rain = weather.contains("rain");
			boolean snow = weather.contains("snow");
			boolean hail = weather.contains("hail");
			boolean thunder = weather.contains("thunder");
			boolean tornado = weather.contains("tornado");
			boolean clear = !fog && !rain && !snow && !hail && !thunder && !tornado;
			collector.emit(new Values
					(input.getStringByField("airport"),
							fog, rain, snow, hail, thunder, tornado, clear));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("airport", "fog", "rain", "snow", "hail", "thunder", "tornado", "clear"));
		}

	}

	static class UpdateCurrentWeatherBolt extends BaseBasicBolt {
		private org.apache.hadoop.conf.Configuration conf;
		private Connection hbaseConnection;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			try {
				conf = HBaseConfiguration.create();
			    conf.set("hbase.zookeeper.property.clientPort", "2181");
			    conf.set("hbase.zookeeper.quorum", StringUtils.join((List<String>)(stormConf.get("storm.zookeeper.servers")), ","));
			    String znParent = (String)stormConf.get("zookeeper.znode.parent");
			    if(znParent == null)
			    	znParent = new String("/hbase");
				conf.set("zookeeper.znode.parent", znParent);
				hbaseConnection = ConnectionFactory.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.prepare(stormConf, context);
		}

		@Override
		public void cleanup() {
			try {
				hbaseConnection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// TODO Auto-generated method stub
			super.cleanup();
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			try {
				Table table = hbaseConnection.getTable(TableName.valueOf("current_weather"));
				Put put = new Put(Bytes.toBytes(input.getStringByField("airport")));
				put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("fog"), Bytes.toBytes(input.getBooleanByField("fog")));
				put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("rain"), Bytes.toBytes(input.getBooleanByField("rain")));
				put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("snow"), Bytes.toBytes(input.getBooleanByField("snow")));
				put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("hail"), Bytes.toBytes(input.getBooleanByField("hail")));
				put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("thunder"), Bytes.toBytes(input.getBooleanByField("thunder")));
				put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("tornado"), Bytes.toBytes(input.getBooleanByField("tornado")));
				put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("clear"), Bytes.toBytes(input.getBooleanByField("clear")));
				table.put(put);
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		Map stormConf = Utils.readStormConfig();
		String zookeepers = StringUtils.join((List<String>)(stormConf.get("storm.zookeeper.servers")), ",");
		System.out.println(zookeepers);
		ZkHosts zkHosts = new ZkHosts(zookeepers);
		
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "weather-events", "/weather-events","weather_id");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		// kafkaConfig.zkServers = (List<String>)stormConf.get("storm.zookeeper.servers");
		kafkaConfig.zkRoot = "/weather-events";
		// kafkaConfig.zkPort = 2181;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("raw-weather-events", kafkaSpout, 1);
		builder.setBolt("filter-airports", new FilterAirportsBolt(), 1).shuffleGrouping("raw-weather-events");
		builder.setBolt("extract-weather", new ExtractWeatherBolt(), 1).shuffleGrouping("filter-airports");
		builder.setBolt("update-current-weather", new UpdateCurrentWeatherBolt(), 1).fieldsGrouping("extract-weather", new Fields("airport"));


		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 4);

		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);
			LocalCluster cluster = new LocalCluster(zookeepers, 2181L);
			cluster.submitTopology("weather-topology", conf, builder.createTopology());
		} 
	} 
}
