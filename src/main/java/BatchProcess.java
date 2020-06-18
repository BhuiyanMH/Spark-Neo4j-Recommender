import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.Arrays;

public class BatchProcess {

    public static String processBatchData(JavaSparkContext spark) {


        JavaRDD<String> productsRdd = spark.textFile("src/main/resources/products.csv").filter(t -> !t.contains("product_id"));
        JavaRDD<String> ordersRDD = spark.textFile("src/main/resources/orders.csv").filter(t -> t.contains(",prior,"));
        JavaRDD<String> orderProductRDD = spark.textFile("src/main/resources/order_products__prior.csv").filter(t -> !t.contains("order_id"));

        //generate order, user_pair
        //<order_id, user_id>
        JavaPairRDD<String, String> orderCustomerPair = ordersRDD.mapToPair(t -> new Tuple2<>(t.split(",")[0], t.split(",")[1]));


        //generate order, product_pair
        //<order_id, <product_id, reorder>>
        JavaPairRDD<String, Tuple2<String, String>> orderProductPair = orderProductRDD.mapToPair(t -> new Tuple2<>(t.split(",")[0], new Tuple2<String, String>(t.split(",")[1], t.split(",")[3])));


        //join the order-customer and order-product rdd
        //result: <order_id, <user_id, <product_id, reorder>>>
        JavaPairRDD combinedPairRDD = orderCustomerPair.join(orderProductPair);

//        List combinedRDDList = combinedPairRDD.take(20);
//        ListIterator combinedRDDListIterator = combinedRDDList.listIterator();
//        while (combinedRDDListIterator.hasNext()){
//            System.out.println(combinedRDDListIterator.next());
//        }
        //convert pair rdd to a rdd of tuple4
        JavaRDD<Tuple4<String, String, String, String>> combinedRDD = combinedPairRDD.map(
                (Function<Tuple2<String, Tuple2<String, Tuple2<String, String>>>, Tuple4<String, String, String, String>>) row -> {
                    return new Tuple4<String, String, String, String>(row._1, row._2._1, row._2._2._1, row._2._2._2);
                }
        );
        combinedRDD.persist(StorageLevel.MEMORY_ONLY());
        JavaRDD saveCombinedRdd = combinedRDD.map(
                (Function<Tuple4<String, String, String, String>, String>) row -> {
                    return row._1()+","+row._2()+","+row._3()+","+row._4();
                }
        );
        //order_id, user_id, product_id, reorder
        saveCombinedRdd.saveAsTextFile("src/main/resources/combined.csv");

        //calculate purchase of product by user
        JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> purchasePairRDD = combinedRDD.mapToPair( row -> new Tuple2<>( new Tuple2(row._2(), row._3()), new Tuple2(1, Integer.parseInt(row._4())) ));


        JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> purchaseRDD = purchasePairRDD.aggregateByKey(
                new Tuple2<>(0, 0),
                (v1, v2) -> (new Tuple2<>(v1._1()+1, v1._2()+v2._2())), //combiner
                (v3, v4) -> (new Tuple2<>(v3._1()+1, v3._2()+v4._2())) //reducer
        );

//        purchaseRDD.foreach(
//                t -> {System.out.println("user: "+t._1()._1() + " product: "+t._1()._2()+" order: "+ t._2()._1() +" reorder: "+t._2()._2());}
//        );

        //user_id, product_id, order_count, reorder_count
        JavaRDD purchaseRelationRDD = purchaseRDD.map(
                (Function< Tuple2<Tuple2<String, String>, Tuple2<Integer, Integer>>, String>) row -> {
                    return row._1()._1()+","+row._1()._2()+","+row._2()._1().toString()+","+row._2()._1();
                }
            );
        purchaseRelationRDD.saveAsTextFile("src/main/resources/purchase_relation.csv");

        processEvents(spark, new ArrayList<String>(Arrays.asList("src/main/resources/events.csv")), "src/main/resources/events_relation.csv");
        return "done!";
    }


    private static boolean processEvents(JavaSparkContext context, ArrayList<String> filePaths, String saveFilePath){

        JavaRDD<String> combinedRDD = context.emptyRDD();

        for(String path: filePaths){

            JavaRDD<String> eventsRDD =  context.textFile(path).filter(t -> !t.contains("event"));

            JavaPairRDD<Tuple3, Integer> eventsPair  = eventsRDD.mapToPair(row -> new Tuple2<>(new Tuple3(row.split(",")[1], row.split(",")[3], row.split(",")[2]), 1));


            JavaPairRDD eventsCountPair = eventsPair.reduceByKey((a, b)-> (a + b));

            JavaRDD<String> eventsRelationRDD = eventsCountPair.map(
                    (Function< Tuple2<Tuple3<String, String, String>, Integer>, String>) row -> {
                        return row._1()._1()+","+row._1()._2()+","+row._1()._3()+","+row._2();
                    }
            );

            combinedRDD = combinedRDD.union(eventsRelationRDD);
        }
        //combinedRDD = combinedRDD.coalesce(1, true);
        combinedRDD.saveAsTextFile(saveFilePath);
        return true;
    }

}

