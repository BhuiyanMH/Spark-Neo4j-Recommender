import org.neo4j.driver.*;

import java.util.HashMap;

public class Recommender {

    private static Driver driver;

    private static HashMap landingRecommendation(String customerID) {

        Session session = null;
        if (driver == null)
            driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123456"));
        session = driver.session();

        String query = "MATCH (U1:Customer{id:\""+customerID+"\"})-[:PURCHASE]->(ph_u1:Product)<-[p:PURCHASE]-(C:Customer)\n" +
                "MATCH (C:Customer)-[:PURCHASE]->(ph_rest:Product)\n" +
                "WHERE ph_u1 <> ph_rest AND U1<>C\n" +
                "RETURN ph_rest.id as productID, ph_rest.name as productName, sum(p.total_order)\n" +
                "ORDER BY sum(p.total_order) DESC LIMIT 10";
        Result result = session.run(query);

        HashMap queryResult = new HashMap();

        while (result.hasNext()){
            Record record = result.next();
            queryResult.put(record.get("productID"), record.get("productName"));
        }
        session.close();
        //driver.close();
        return queryResult;
    }

    private static HashMap viewRecommendation(String customerID, String productID) {

        Session session = null;
        if (driver == null)
            driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123456"));
        session = driver.session();

        String query = "MATCH (c:Customer{id:\""+customerID+"\"}) -[:INTERECT]->(p:Product{id:\""+productID+"\"})<-[:PURCHASE]-(c2:Customer)\n" +
                "MATCH (p)-[:BELONGS_TO]->(cat:Category)\n" +
                "WITH c2, cat\n" +
                "MATCH (c2) -[:INTERECT]-(rp:Product)-[:BELONGS_TO]-(cat)\n" +
                "RETURN rp.name as productName, rp.id as productID\n" +
                "ORDER BY rp.total_order";
        Result result = session.run(query);

        HashMap queryResult = new HashMap();

        while (result.hasNext()){
            Record record = result.next();
            queryResult.put(record.get("productID"), record.get("productName"));
        }
        session.close();
        //driver.close();
        return queryResult;
    }

    private static HashMap cartRecommendation(String customerID, String productID) {

        Session session = null;
        if (driver == null)
            driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "123456"));
        session = driver.session();

        String query = "MATCH (c:Customer{id:\""+customerID+"\"}) -[:INTERECT]->(p:Product{id:\""+productID+"\"})<-[:PURCHASE]-(c2:Customer)\n" +
                "MATCH (p)-[:BELONGS_TO]->(cat:Category)\n" +
                "WITH p, c2, cat\n" +
                "MATCH (c2) -[:PURCHASE]-(rp:Product)-[:BELONGS_TO]-(cat)\n" +
                "WHERE p <> rp\n" +
                "RETURN rp.id as productID, rp.name as productName\n" +
                "ORDER BY rp.total_order LIMIT 10";
        Result result = session.run(query);

        HashMap queryResult = new HashMap();

        while (result.hasNext()){
            Record record = result.next();
            queryResult.put(record.get("productID"), record.get("productName"));
        }
        session.close();
        driver.close();
        return queryResult;
    }


    public static void main(String args[]){

        HashMap resultLanding = landingRecommendation("106161");
        System.out.println("LANDING RECOMMENDATION");
        for(Object key: resultLanding.keySet()){
            System.out.println("Product ID: " + key + " Product Name: "+ resultLanding.get(key));
        }

        HashMap resultView = viewRecommendation("53151", "44142");
        System.out.println("VIEW RECOMMENDATION");
        for(Object key: resultView.keySet()){
            System.out.println("Product ID: " + key + " Product Name: "+ resultView.get(key));
        }

        HashMap resultCart = cartRecommendation("53151", "44142");
        System.out.println("ADD to CART RECOMMENDATION");
        for(Object key: resultCart.keySet()){
            System.out.println("Product ID: " + key + " Product Name: "+ resultCart.get(key));
        }


    }
}
