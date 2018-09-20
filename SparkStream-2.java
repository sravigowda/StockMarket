import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
import com.sun.mail.imap.protocol.Item;

import scala.Tuple2;

public class SparkStream {
	
	public class JsonStock implements Comparable<JsonStock>,Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String symbol;
		private String timestamp;
		private pricedata priceData;
		@Override
	    public String toString() {
	        return symbol + " - " + timestamp + " (" + priceData + ")";
	    }
		
		@Override
	    public int compareTo(JsonStock jsonstock) {

			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			java.util.Date parsedTimeStamp = null,jsonTimeStamp = null;
			try {
				parsedTimeStamp = dateFormat.parse(this.timestamp);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			try {
				jsonTimeStamp = dateFormat.parse(jsonstock.timestamp);
			} catch (ParseException e) {
				e.printStackTrace();
			}	
			return parsedTimeStamp.compareTo(jsonTimeStamp);
		}
	}
	
	public class pricedata implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private Float open;
		private Float high;
		private Float low;
		private Float close;
		private Integer volume;
		@Override
	    public String toString() {
	        return open + " - " + high + " - " +low + " - " + close  + " - " + volume;
	    }
	}


	
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication")
				.set("spark.driver.allowMultipleContexts", "true");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		

		Logger.getRootLogger().setLevel(Level.ERROR);

		JavaDStream<String> newlines = jssc.textFileStream("/Users/ravig/Documents/programs/stock_data");
		//JavaDStream<String> newDstream=newlines.window(Durations.seconds(600),Durations.seconds(300));
		JavaDStream<String> newDstream = newlines.window(Durations.seconds(120), Durations.seconds(60));

		newDstream.print();
		
		ArrayList<JsonStock> list = new ArrayList<JsonStock>();

		DStream<Tuple2<String, Float>> Close_Dstream = newDstream.flatMap(new FlatMapFunction<String, JsonStock>() {

			private static final long serialVersionUID = 1L;
			
			public Iterator<JsonStock> call(String x) throws Exception {
				JSONParser jsonParser = new JSONParser();
				
				Gson gson = new Gson();

				try {
					Object obj = jsonParser.parse(x);
					JSONArray jsonstockcontent = (JSONArray) obj;
					
					for (Object obj1:jsonstockcontent) {
					JsonStock convertstock = gson.fromJson(obj1.toString(), JsonStock.class);
					list.add(convertstock);
					}	
				} catch (Exception e) {
					e.printStackTrace();
				}
				return list.iterator();
			}

		}).mapToPair(x -> new Tuple2<String, Float>(x.symbol, x.priceData.close))
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

		System.out.println("Following is the simple moving average closing prices of the stocks");
		Close_Dstream.print();
		Close_Dstream.saveAsTextFiles("Close", "SimpleMovingAverage");
		
		

		DStream<Tuple2<String, Float>> Gain_Dstream = newDstream.flatMap(new FlatMapFunction<String, JsonStock>() {

			private static final long serialVersionUID = 1L;

			public Iterator<JsonStock> call(String x) throws Exception {
				JSONParser jsonParser = new JSONParser();
				
				Gson gson = new Gson();

				try {
					Object obj = jsonParser.parse(x);
					JSONArray jsonstockcontent = (JSONArray) obj;
					
					for (Object obj1:jsonstockcontent) {
					JsonStock convertstock = gson.fromJson(obj1.toString(), JsonStock.class);
					list.add(convertstock);
					}	
				} catch (Exception e) {
					e.printStackTrace();
				}
				return list.iterator();
			}
		}).mapToPair(x -> new Tuple2<String, Float>(x.symbol,(x.priceData.close - x.priceData.open)))
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

		System.out.println("Following is the profit of the stocks");
		Gain_Dstream.print();
		System.out.print("Stock which provided maximum profit is ");
		Gain_Dstream.print(1);
		Gain_Dstream.saveAsTextFiles("Profit", "Stocks");
		

		DStream<Tuple2<String, Integer>> Volume_Dstream = newDstream.flatMap(new FlatMapFunction<String, JsonStock>() {

			private static final long serialVersionUID = 1L;

			public Iterator<JsonStock> call(String x) throws Exception {
				JSONParser jsonParser = new JSONParser();
				
				Gson gson = new Gson();

				try {
					Object obj = jsonParser.parse(x);
					JSONArray jsonstockcontent = (JSONArray) obj;
					
					for (Object obj1:jsonstockcontent) {
					JsonStock convertstock = gson.fromJson(obj1.toString(), JsonStock.class);
					list.add(convertstock);
					}	
				} catch (Exception e) {
					e.printStackTrace();
				}
				return list.iterator();
			}
		}).mapToPair(x -> new Tuple2<String, Integer>(x.symbol, x.priceData.volume))
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
						System.out.println("Trading volume of the stocks are inside");
						return item.swap();
					}
				}).dstream();

		
		System.out.println("Trading volume of the stocks are");
		Volume_Dstream.print();
		System.out.print("Stock which can be purchased is ");
		Volume_Dstream.print(1);
		Volume_Dstream.saveAsTextFiles("Volume", "Stocks");
		
		JavaDStream<JsonStock> RSI_Dstream = newDstream.flatMap(new FlatMapFunction<String, JsonStock>() {
			private static final long serialVersionUID = 1L;
			
				
			public Iterator<JsonStock> call(String x) throws Exception {
				JSONParser jsonParser = new JSONParser();
				
				Gson gson = new Gson();

				try {
					Object obj = jsonParser.parse(x);
					JSONArray jsonstockcontent = (JSONArray) obj;
					
					for (Object obj1:jsonstockcontent) {
					JsonStock convertstock = gson.fromJson(obj1.toString(), JsonStock.class);
					list.add(convertstock);
					}	
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				return list.iterator();
			}
			
		});
		System.out.println(RSI_Dstream.toString());
		JavaDStream<Long> count_stream = RSI_Dstream.count(); 
		System.out.print("Number of RDD in this stream = ");
		count_stream.print();
		 
		
		DStream<Tuple2<String, Float>> mapstream= RSI_Dstream.mapToPair((x)->new Tuple2<String, JsonStock>(x.symbol,x))
				.mapToPair(new PairFunction<Tuple2<String, JsonStock>, JsonStock, String>() {
				
					private static final long serialVersionUID = 1L;
					
					@Override
					public Tuple2<JsonStock, String> call(Tuple2<String, JsonStock> item) throws Exception {
						// TODO Auto-generated method stub
						return item.swap();
					}
				}).transformToPair(x -> x.sortByKey())
				.mapToPair(new PairFunction<Tuple2<JsonStock, String >,  String,JsonStock>() {
					
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, JsonStock> call(Tuple2<JsonStock, String> item) throws Exception {
						// TODO Auto-generated method stub
						return item.swap();
					}
				}).mapToPair((x) -> new Tuple2<String, Float>(x._1,x._2.priceData.close)).groupByKey().
				//updateStateByKey(Function2<List<iterable<Float>>,Optional<S>,Optional<S>> mypdate)
				mapToPair(getRSI).
				dstream();
		System.out.println("RSI values for various stocks are");
		mapstream.print();
		mapstream.saveAsTextFiles("RSI", "Stocks");
		
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
	
	public int sortFunction(JsonStock x, JsonStock y) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		try {
			return dateFormat.parse(x.timestamp).compareTo(dateFormat.parse(y.timestamp));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
		
	}
	
	private static PairFunction<Tuple2<String,Iterable<Float>>, String, Float> getRSI = (tuple) -> {
		String Symbol = tuple._1;
		Iterable <Float> close_values = tuple._2;
		List<Float> data = new ArrayList<>();
		for(Float i: close_values) {
			data.add(i);
		}
		 
		
		int lastBar = data.size() - 1;
		int periodLength = lastBar;
		int firstBar = lastBar - periodLength + 1;
		if (firstBar < 0) {
			String msg = "Quote history length " + data.size() + " is insufficient to calculate the indicator.";
			throw new Exception(msg);
		}

		double aveGain = 0, aveLoss = 0;
		for (int bar = firstBar + 1; bar <= lastBar; bar++) {
			double change = data.get(bar) - data.get(bar - 1);
			if (change >= 0) {
				aveGain += change;
			} else {
				aveLoss += change;
			}
		}

		Float rs = (float) (aveGain / Math.abs(aveLoss));
		Float rsi = 100 - 100 / (1 + rs);
		
		Tuple2<String,Float> RSI = new Tuple2<String, Float>(Symbol,rsi);
		return RSI;
		
	};

}
