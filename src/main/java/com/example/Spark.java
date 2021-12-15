package com.example;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.Data;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class Spark {
    private JavaSparkContext sc;
    private SparkConf sparkConf;

    public static CsvParser getCsvParser(){
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        return new CsvParser(settings);
    }

    private void show(JavaRDD<?> rdd){
        show(rdd, 10);
    }
    private void show(JavaRDD<?> rdd, int count){
        rdd.take(count).stream().forEach(System.out::println);
    }

    private void initSparkSession(){
        sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setMaster("local");
        sc = new JavaSparkContext(sparkConf);
    }

    @Data
    public static class Stock implements Serializable {
        private String date;
        private Double open; private Double high;
        private Double low;
        private Double close;
        private Double volume;
        private Double adjclose;
        private String symbol;
    }

    private static Double toDouble(String s){
        try{
            return Double.valueOf(s);
        }catch (Exception e){
            return null;
        }
    }

//    private static Stock toStock(String line){
//        final CsvParser csvParser = getCsvParser();
//        final String[] tokens = csvParser.parseLine(line);
//        return toStock(tokens);
//    }

    private static Stock toStock(String[] tokens){
        final Stock stock = new Stock();
        stock.setDate(tokens[0]);
        stock.setOpen(toDouble(tokens[1]));
        stock.setClose(toDouble(tokens[2]));
        stock.setHigh(toDouble(tokens[3]));
        stock.setLow(toDouble(tokens[4]));
        stock.setVolume(toDouble(tokens[5]));
        stock.setAdjclose(toDouble(tokens[6]));
        stock.setSymbol(tokens[7]);
        return stock;
    }

    @Data
    public static class StockStats implements Serializable{
        private String symbol;
        private Double avgReturn;
        private Integer count;
    }

    public void start(String[] args) throws ArgumentParserException {


        initSparkSession();
        final String inputDir = args[0];
        final String filePath = Paths.get(inputDir).resolve("stocks.csv").toString();
        System.out.println("File path: " + filePath);

        System.out.println("--------------------------Base RDD--------------------------");

        JavaRDD<String> rawStockRDD = sc.textFile(filePath);
        show(rawStockRDD);

        System.out.println("----------------- Convert each line to Stock class object -----------------------------");

        final JavaRDD<Stock> stockJavaRDD = rawStockRDD
                .mapPartitions(iterator -> {
                    final CsvParser csvParser = getCsvParser();
                    final List<Stock> batch = new LinkedList<>();
                    while(iterator.hasNext()){
                        final String line = iterator.next();
                        if(!line.startsWith("date")) {
                            final Stock stock = toStock(csvParser.parseLine(line));
                            batch.add(stock);
                        }
                    }
                    return batch.iterator();
                }).cache();
        show(stockJavaRDD);


        System.out.println("----------------- How many records are there in the stock class RDD -----------------------------");
        long count = stockJavaRDD.count();
        System.out.println("Total record count: "+count);

        System.out.println("----------------- Convert each line to Stock class object -----------------------------");
        long recCount2016 = stockJavaRDD.filter(line -> line.getDate().startsWith("2016")).count();
        System.out.println("Count of records in stocks file for thr year 2016: " +recCount2016);

        System.out.println("----------------- How many records are there in 2016 and for stock INTC -----------------------------");
        long recCount2016INTC = stockJavaRDD.filter(line -> line.getDate().startsWith("2016") && "INTC".equals(line.getSymbol())).count();
        System.out.println("Count of records in 2016 and for stock INTC: "+ recCount2016INTC);

        System.out.println("----------------- What percentage of days INTC stocks ended the day in positive gain in 2016 -----------------------------");
        JavaRDD<Stock> stockIntc2016RDD = stockJavaRDD.filter(line -> line.getDate().startsWith("2016") && "INTC".equals(line.getSymbol()));
        long count1 = stockIntc2016RDD.filter(line -> line.getClose() > line.getOpen()).count();
        //System.out.println(count1);
        long l = count1 * 100/ recCount2016INTC;
        System.out.println("Percentage of days INTC stocks ended the day in positive gain in 2016: "+ l);

        System.out.println("----------------- Find top 3 records based with highest trading volume in 2016 -----------------------------");

        JavaRDD<Stock> stock2016RDD = stockJavaRDD.filter(line -> line.getDate().startsWith("2016"));
        List<Stock> top3Vol = stock2016RDD.sortBy(Stock::getVolume, false, 1).take(3);
        top3Vol.stream().map(line -> String.format("%s -> %.2f", line.getSymbol(), line.getVolume())).forEach(System.out::println);

        System.out.println("----------------- Find top 3 records based with highest trading volume in 2016 -----------------------------");
        final JavaPairRDD<Double, StockStats> topStocksByAvgReturn = stock2016RDD.groupBy(Stock::getSymbol).map(tuple -> {
            final String symbol = tuple._1();
            final Iterable<Stock> batch = tuple._2();
            double sum = 0.0;
            int batchCount = 0;
            for(Stock stock : batch) {
                Double open = stock.getOpen();
                Double close = stock.getClose();
                Double dailyReturn = 100 * (close - open) / open;
                ++batchCount;
                sum += dailyReturn;
                }
                final double avg = sum / batchCount;
                final StockStats stats = new StockStats();
                stats.setAvgReturn(avg);
                stats.setCount(batchCount);
                stats.setSymbol(symbol);
                return stats;
        }).keyBy(StockStats::getAvgReturn)
                .sortByKey(false);

        topStocksByAvgReturn.map(Tuple2::_2).take(3).forEach(System.out::println);
    }





    public static void main(String[] args) throws ArgumentParserException {
        System.out.println("Starting application.");
        new Spark().start(args);
    }
}
