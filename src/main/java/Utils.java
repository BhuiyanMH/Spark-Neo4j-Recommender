import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.ListIterator;

public class Utils {

    public static void printPairRdd(JavaPairRDD<String, String> rdd){
        rdd.foreach(data -> {
            System.out.println("key="+data._1() + " value=" + data._2());
        });
    }

    public static void printSomePairRdd(ListIterator<Tuple2<String, String>> values){

        while(values.hasNext()) {

            Tuple2<String, String> data = values.next();
            System.out.println("key="+data._1() + " value=" + data._2());

        }

    }

    public static ArrayList<String> cleanRow(String row)
    {
        ArrayList<String> columns = new ArrayList<String>();

        boolean quoted = false;
        int startIndex =0, endIndex=0;
        for(int i=0; i<row.length()-1; i++)
        {
            if(row.charAt(i)==',' && !quoted)
            {
                columns.add(row.substring(startIndex, i));
                startIndex = i+1;
            }
            else if(row.charAt(i)=='"')
                quoted=!quoted;
        }
        columns.add(row.substring(startIndex));
        return columns;
    }

}

