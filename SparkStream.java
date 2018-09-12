import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

import com.google.gson.Gson;

import scala.Tuple2;

public class SparkStream {

	static class StockRDD implements Serializable {
		private static final long serialVersionUID = -2685444218382696366L;
		String StockName;
		private Float Openprice;
		private Float Closeprice;
		private Integer Volume;

		public StockRDD(String StockName) {
			this.StockName = StockName;
		}

		public void setOpenprice(float Openprice) {
			this.Openprice = Openprice;
		}

		public void setCloseprice(float Closeprice) {
			this.Closeprice = Closeprice;
		}

		public void setVolume(int Volume) {
			this.Volume = Volume;
		}

		public Float getOpenprice() {
			return Openprice;
		}

		public Float getCloseprice() {
			return Closeprice;
		}

		public int getVolume() {
			return Volume;
		}

		public String getStockName() {
			return StockName;
		}
	}

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication")
				.set("spark.driver.allowMultipleContexts", "true");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));

		Logger.getRootLogger().setLevel(Level.ERROR);

		JavaDStream<String> newlines = jssc.textFileStream("/Users/ravig/Documents/programs/stock_data");
		JavaDStream<String> newDstream=newlines.window(Durations.seconds(600),Durations.seconds(300));
		//JavaDStream<String> newDstream = newlines.window(Durations.seconds(120), Durations.seconds(60));

		newDstream.print();
		

		DStream<Tuple2<String, Float>> Close_Dstream = newDstream.flatMap(new FlatMapFunction<String, StockRDD>() {

			private static final long serialVersionUID = 1L;

			public Iterator<StockRDD> call(String x) throws Exception {
				JSONParser jsonParser = new JSONParser();
				ArrayList<StockRDD> list = new ArrayList<StockRDD>();

				try {
					Object obj = jsonParser.parse(x);
					JSONArray stockcontent = (JSONArray) obj;
					for (int i = 0; i < stockcontent.size(); i++) {
						JSONObject stock = (JSONObject) stockcontent.get(i);

						StockRDD stock_index = new StockRDD(((String) stock.get("symbol")));

						//System.out.println((String) stock.get("symbol") + " ");

						Map priceData = ((Map) stock.get("priceData"));
						Iterator<Map.Entry> itr1 = priceData.entrySet().iterator();
						while (itr1.hasNext()) {
							Map.Entry pair = itr1.next();
							//System.out.println(pair.getKey() + " : " + pair.getValue() + " ");

							if ((pair.getKey().toString()).equals("close")) {
								stock_index.setCloseprice(Float.parseFloat(pair.getValue().toString()));
							}
							if ((pair.getKey().toString()).equals("open")) {
								stock_index.setOpenprice(Float.parseFloat(pair.getValue().toString()));
							}
							if ((pair.getKey().toString()).equals("volume")) {
								stock_index.setVolume(Integer.parseInt(pair.getValue().toString()));
							}

						}
						list.add(stock_index);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return list.iterator();
			}
		}).mapToPair(x -> new Tuple2<String, Float>(x.StockName, x.Closeprice))
				.mapValues(value -> new Tuple2<Float, Integer>(value, 1))
				.reduceByKey((tuple1, tuple2) -> new <String, Tuple2<Float, Integer>>Tuple2<Float, Integer>(
						tuple1._1 + tuple2._1, tuple1._2 + tuple2._2))
				.mapToPair(getAverageByKey).mapToPair(new PairFunction<Tuple2<String, Float>, Float, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Float, String> call(Tuple2<String, Float> item) throws Exception {
						return item.swap();
					}
				}).transformToPair(x -> x.sortByKey(false))
				.mapToPair(new PairFunction<Tuple2<Float, String>, String, Float>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Float> call(Tuple2<Float, String> item) throws Exception {
						return item.swap();
					}
				}).dstream();

		Close_Dstream.print();

		DStream<Tuple2<String, Float>> Gain_Dstream = newDstream.flatMap(new FlatMapFunction<String, StockRDD>() {

			private static final long serialVersionUID = 1L;

			public Iterator<StockRDD> call(String x) throws Exception {
				// TODO Auto-generated method stub
				JSONParser jsonParser = new JSONParser();
				ArrayList<StockRDD> list = new ArrayList<StockRDD>();

				try {
					Object obj = jsonParser.parse(x);
					JSONArray stockcontent = (JSONArray) obj;
					for (int i = 0; i < stockcontent.size(); i++) {
						JSONObject stock = (JSONObject) stockcontent.get(i);

						StockRDD stock_index = new StockRDD(((String) stock.get("symbol")));

						//System.out.println((String) stock.get("symbol") + " ");

						Map priceData = ((Map) stock.get("priceData"));
						Iterator<Map.Entry> itr1 = priceData.entrySet().iterator();
						while (itr1.hasNext()) {
							Map.Entry pair = itr1.next();
							//System.out.println(pair.getKey() + " : " + pair.getValue() + " ");

							if ((pair.getKey().toString()).equals("close")) {
								stock_index.setCloseprice(Float.parseFloat(pair.getValue().toString()));
							}
							if ((pair.getKey().toString()).equals("open")) {
								stock_index.setOpenprice(Float.parseFloat(pair.getValue().toString()));
							}
							if ((pair.getKey().toString()).equals("volume")) {
								stock_index.setVolume(Integer.parseInt(pair.getValue().toString()));
							}

						}
						list.add(stock_index);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return list.iterator();
			}
		}).mapToPair(x -> new Tuple2<String, Float>(x.StockName, (x.Closeprice - x.Openprice)))
				.mapValues(value -> new Tuple2<Float, Integer>(value, 1))
				.reduceByKey((tuple1, tuple2) -> new <String, Tuple2<Float, Integer>>Tuple2<Float, Integer>(
						tuple1._1 + tuple2._1, tuple1._2 + tuple2._2))
				.mapToPair(getAverageByKey).mapToPair(new PairFunction<Tuple2<String, Float>, Float, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Float, String> call(Tuple2<String, Float> item) throws Exception {
						return item.swap();
					}
				}).transformToPair(x -> x.sortByKey(false))
				.mapToPair(new PairFunction<Tuple2<Float, String>, String, Float>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Float> call(Tuple2<Float, String> item) throws Exception {
						return item.swap();
					}
				}).dstream();

		Gain_Dstream.print();
		Gain_Dstream.print(1);

		DStream<Tuple2<String, Integer>> Volume_Dstream = newDstream.flatMap(new FlatMapFunction<String, StockRDD>() {

			private static final long serialVersionUID = 1L;

			public Iterator<StockRDD> call(String x) throws Exception {
				// TODO Auto-generated method stub
				JSONParser jsonParser = new JSONParser();
				ArrayList<StockRDD> list = new ArrayList<StockRDD>();

				try {
					Object obj = jsonParser.parse(x);
					JSONArray stockcontent = (JSONArray) obj;
					for (int i = 0; i < stockcontent.size(); i++) {
						JSONObject stock = (JSONObject) stockcontent.get(i);

						StockRDD stock_index = new StockRDD(((String) stock.get("symbol")));

						//System.out.println((String) stock.get("symbol") + " ");

						Map priceData = ((Map) stock.get("priceData"));
						Iterator<Map.Entry> itr1 = priceData.entrySet().iterator();
						while (itr1.hasNext()) {
							Map.Entry pair = itr1.next();
							//System.out.println(pair.getKey() + " : " + pair.getValue() + " ");

							if ((pair.getKey().toString()).equals("close")) {
								stock_index.setCloseprice(Float.parseFloat(pair.getValue().toString()));
							}
							if ((pair.getKey().toString()).equals("open")) {
								stock_index.setOpenprice(Float.parseFloat(pair.getValue().toString()));
							}
							if ((pair.getKey().toString()).equals("volume")) {
								stock_index.setVolume(Integer.parseInt(pair.getValue().toString()));
							}

						}
						list.add(stock_index);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return list.iterator();
			}
		}).mapToPair(x -> new Tuple2<String, Integer>(x.StockName, x.Volume))
				.mapValues(value -> new Tuple2<Integer, Integer>(value, 1))
				.reduceByKey((tuple1, tuple2) -> new <String, Tuple2<Integer, Integer>>Tuple2<Integer, Integer>(
						tuple1._1 + tuple2._1, tuple1._2 + tuple2._2))
				.mapToPair(getTotalByKey).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
						return item.swap();
					}
				}).transformToPair(x -> x.sortByKey(false))
				.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
						return item.swap();
					}
				}).dstream();

		Volume_Dstream.print();
		Volume_Dstream.print(1);

		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}

	private static PairFunction<Tuple2<String, Tuple2<Float, Integer>>, String, Float> getAverageByKey = (tuple) -> {
		Tuple2<Float, Integer> val = tuple._2;
		Float total = val._1;
		Integer count = val._2;
		Tuple2<String, Float> averagePair = new Tuple2<String, Float>(tuple._1, total / count);
		return averagePair;
	};

	private static PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Integer> getAverageByKey_Int = (
			tuple) -> {
		Tuple2<Integer, Integer> val = tuple._2;
		Integer total = val._1;
		Integer count = val._2;
		Tuple2<String, Integer> averagePair = new Tuple2<String, Integer>(tuple._1, total / count);
		return averagePair;
	};
	
	private static PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Integer> getTotalByKey = (
			tuple) -> {
		Tuple2<Integer, Integer> val = tuple._2;
		Integer total = val._1;
		Tuple2<String, Integer> averagePair = new Tuple2<String, Integer>(tuple._1, total);
		return averagePair;
	};
}
