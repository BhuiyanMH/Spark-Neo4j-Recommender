import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Main {
	
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));

        SparkConf conf = new SparkConf().setAppName("Agrolytics-Recommender").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
		ctx.setLogLevel("ERROR");

		if (args[0].equals("recommender")) {
            System.out.println(BatchProcess.processBatchData(ctx));
        }
		else {
			throw new Exception("Wrong argument!");
		}
	}
}

