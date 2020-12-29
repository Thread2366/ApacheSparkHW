import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

public class SparkApplication {
    public static void main(String[] args) throws FileSystemException {

        if (args.length < 3) {
            throw new RuntimeException("Usage: java -jar SparkApplication.jar inputFile.csv outputDirectory scale");
        }

        int scale = Integer.parseInt(args[2]);
        RunJob("yarn", args[0], args[1], scale);
    }

    public static void RunJob(String master, String input, String output, Integer scale) {

        // Значение Scale по которому агрегируются метрики не должно быть 0
        if (scale == 0)
            throw new IllegalArgumentException("Scale must be non zero");

        // Создание SparkContext
        SparkContext sc = new SparkContext(master, "hw2", new SparkConf(true));


        JavaRDD<String> results = sc.textFile(input, 4).toJavaRDD()  //входной файл загружается в Spark RDD
                .map(row -> ConvertMetric(row)) // из CSV строк извлекаются числовые значения метрик
                .filter(x -> x != null) // строки, которые не удалось распарсить, отбрасываются
                .map(x -> Arrays.asList(x.get(0), x.get(1) / scale * scale, x.get(2))) // timestamp округляется до ближайшей секунды, кратной scale
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x.get(0), x.get(1)), x.get(2))) // ключ - это кортеж из metricId и timestamp, значение - value
                .groupByKey(4)
                .map(x -> String.format("%s, %s, %s, %s", x._1._1, x._1._2, scale,
                        StreamSupport.stream(x._2.spliterator(), false)
                                .mapToInt(a -> a)
                                .average()
                                .getAsDouble())); //для каждой группы подсчитывается среднее по value

        results.saveAsTextFile(output); //вывод результатов в выходную папку
    }

    public static List<Integer> ConvertMetric(String csvRow) {
        if (csvRow == null) return null;
        String[] tokens = csvRow.split(", *");
        if (tokens.length != 3) return null;
        List<Integer> result = new ArrayList<>();
        for (String token: tokens) {
            int elem;
            try {
                elem = Integer.parseInt(token);
            } catch (NumberFormatException e) {
                return null;
            }
            result.add(elem);
        }
        return result;
    }
}
