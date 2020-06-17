import com.opencsv.CSVReaderHeaderAware;
import org.apache.spark.sql.sources.In;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;

import static org.neo4j.driver.Values.parameters;

import com.opencsv.CSVReader;
import org.neo4j.driver.types.Node;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class LoadGraph {

    private static Driver driver;

    public static void excCypher(String query) {
        try (Session session = driver.session()) {
            String result = session.writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(Transaction tx) {
                    Result result = tx.run(query);
                    return result.toString();
                }
            });
        }
    }

    private static void loadCategory() {
        String query = "LOAD CSV WITH HEADERS FROM \"file:///recommender/aisles.csv\" as row FIELDTERMINATOR ','\n" +
                "CREATE (c:Category{id:toInteger(row.aisle_id), name:row.aisle})";
        excCypher(query);
    }

    private static HashMap<String, String> loadProduct() {

        HashMap categoryNameMap = new HashMap();

        String categoryFile = "src/main/resources/aisles.csv";
        BufferedReader categoryReader = null;
        String row = "";

        try {
            categoryReader = new BufferedReader(new FileReader(categoryFile));
            while ((row = categoryReader.readLine()) != null) {

                String catId = row.split(",")[0];
                String catName = row.split(",")[1];
                categoryNameMap.put(catId, catName);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (categoryReader != null) {
                try {
                    categoryReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        HashMap<String, String> relationMap = new HashMap<>();

        try (Session session = driver.session()) {
            String message = session.writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(Transaction tx) {
                    String filePath = "src/main/resources/products.csv";
                    BufferedReader br = null;
                    String line = "";
                    Result result = null;
                    boolean skip = true;

                    try {

                        br = new BufferedReader(new FileReader(filePath));
                        while ((line = br.readLine()) != null) {

                            if (skip) { //skip the header
                                skip = false;
                                continue;
                            }
                            ArrayList<String> columns = Utils.cleanRow(line);
                            //System.out.println(columns.get(0)+"-"+columns.get(1)+"-"+columns.get(3));
                            String productID = columns.get(0);
                            String productName = columns.get(1);
                            String categoryId = columns.get(2);

                            relationMap.put(productID, categoryId);

                            Map<String, Object> params = new HashMap<>();
                            params.put("id", productID);
                            params.put("name", productName);
                            params.put("catId", categoryId);
                            params.put("catName", categoryNameMap.get(categoryId));

                            result = tx.run("CREATE (p:Product{id:$id, name:$name}) " +
                                    "MERGE (c:Category{id:$catId, name:$catName}) "+
                                    "CREATE (p)-[:BELONGS_TO]->(c)" , params);
                        }

                    } catch (IOException e) {
                        System.out.println("EXCEPTION OCCURED");
                        e.printStackTrace();
                    } finally {
                        return result.toString();
                    }

                }
            });
            return relationMap;
        }
    }


    public static void main(String... args) throws Exception {
        try {

            driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123456"));

            //loadCategory();
            HashMap productCategories = loadProduct();

//            for (Object key : productCategories.keySet()) {
//                System.out.println("key: " + key + " value: " + productCategories.get(key));
//            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            driver.close();
        }

    }
}