import org.neo4j.driver.*;

import java.io.*;
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

    private static boolean loadProduct() {

        //read the categories and their id from the csv file and save in a map for lookup
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

        //read the products from csv and insert them in to the graph
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

                            Map<String, Object> params = new HashMap<>();
                            params.put("id", productID);
                            params.put("name", productName);
                            params.put("catId", categoryId);
                            params.put("catName", categoryNameMap.get(categoryId));

                            result = tx.run("CREATE (p:Product{id:$id, name:$name}) " +
                                    "MERGE (c:Category{id:$catId, name:$catName}) " +
                                    "CREATE (p)-[:BELONGS_TO]->(c)", params);
                        }

                    } catch (IOException e) {
                        System.out.println("EXCEPTION OCCURED");
                        e.printStackTrace();
                    } finally {
                        return result.toString();
                    }

                }
            });
            return true;
        }
    }

    private static boolean loadPurchaseRelation() {

        //get all the partial file names
        ArrayList<String> partialFiles = getPartialFilePaths("src/main/resources/purchase_relation.csv");
        int count = 0;
        for (String path : partialFiles) {

            System.out.println("Processing file: " + path);
            //for each file read the file, and create the relationships in the graphs

            BufferedReader bufferedReader = null;
            String line = "";
            Result result = null;
            Session session = null;

            try {

                bufferedReader = new BufferedReader(new FileReader(path));
                session = driver.session();
                if (driver == null)
                    driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123456"));

                while ((line = bufferedReader.readLine()) != null) {

                    //user_id, product_id, order_count, reorder_count
                    String customerID = line.split(",")[0];
                    String productID = line.split(",")[1];
                    String orderCount = line.split(",")[2];
                    String reorderCount = line.split(",")[3];

                    String query = "MATCH(p:Product{id:\"" + productID + "\"}) " +
                            "MERGE(c:Customer{id:\"" + customerID + "\"}) " +
                            "CREATE (c)-[:PURCHASE{total_order:toInteger(\"" + orderCount + "\"),reorder_count:toInteger(\"" + reorderCount + "\")}]->(p)";
                    session.run(query);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                session.close();
                driver.close();
            }
        }
        System.out.println("Total relation count: " + count);
        return true;
    }

    private static boolean loadInteractionRelation() {

        //get all the partial file names
        ArrayList<String> partialFiles = getPartialFilePaths("src/main/resources/events_relation.csv");
        for (String path : partialFiles) {

            System.out.println("Processing file: " + path);
            //for each file read the file, and create the relationships in the graphs

            BufferedReader bufferedReader = null;
            String line = "";
            Result result = null;
            Session session = null;

            try {

                bufferedReader = new BufferedReader(new FileReader(path));
                session = driver.session();
                if (driver == null)
                    driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123456"));

                while ((line = bufferedReader.readLine()) != null) {

                    //customer_id, product_id, event, count
                    String customerID = line.split(",")[0];
                    String productID = line.split(",")[1];
                    String event = line.split(",")[2];
                    String count = line.split(",")[3];

                    String query = "MATCH(p:Product{id:\"" + productID + "\"}) " +
                            "MATCH(c:Customer{id:\"" + customerID + "\"}) ";
                    if (event.equals("view")) {
                        String part = "MERGE (c)-[i:INTERECT]->(p)" +
                                " SET i.view_count=toInteger(\"" + count + "\")";
                        query = query + part;
                    } else if (event.equals("addtocart")) {
                        String part = "MERGE (c)-[i:INTERECT]->(p)" +
                                " SET i.atc_count=toInteger(\"" + count + "\")";
                        query = query + part;
                    } else if (event.equals("transaction")) {
                        String part = "MERGE (c)-[i:INTERECT]->(p)" +
                                " SET i.trans_count=toInteger(\"" + count + "\")";
                        query = query + part;
                    } else {
                        continue;
                    }
                    session.run(query);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                session.close();
                driver.close();
            }
        }
        return true;
    }

    private static boolean createSimilarityRelation() {

        Session session = null;
        if (driver == null)
            driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123456"));
        session = driver.session();

        String query = "MATCH(c:Customer)-[:PURCHASE]->(product) " +
                "WITH {item:id(c), categories:collect(id(product))} AS userData " +
                "WITH collect(userData) as data " +
                "CALL algo.similarity.jaccard(data, {topK:3, similarityCutoff:0.1, write:true}) " +
                "YIELD nodes, similarityPairs, writeRelationshipType, writeProperty, min, max, mean, p75 " +
                "RETURN nodes, similarityPairs, writeRelationshipType, writeProperty, min, max, mean, p75";
        session.run(query);

        session.close();
        driver.close();

        return true;
    }

    private static ArrayList<String> getPartialFilePaths(String directory) {
        ArrayList<String> partialFilePaths = new ArrayList<>();
        File[] files = new File(directory).listFiles();
        for (File file : files) {

            if (file.isFile()) {
                String path = file.getPath();
                if (!(path.contains("SUCCESS") || path.contains("crc"))) {
                    partialFilePaths.add(path);
                }
            }
        }
        return partialFilePaths;
    }

    public static void main(String... args) throws Exception {
        try {

            driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123456"));

            //loadCategory();
            //loadProduct();
            //loadPurchaseRelation();
            //loadInteractionRelation();
            createSimilarityRelation();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            driver.close();
        }

    }
}