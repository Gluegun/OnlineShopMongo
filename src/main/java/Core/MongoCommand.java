package Core;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

public class MongoCommand {


    public static void addShop(String console, MongoCollection<Document> shopCollection) {

        String[] split = console.split("\\s", 2);
        String shopName = split[1];

        FindIterable<Document> shops = shopCollection.find();
        for (Document shop : shops) {
            String shopFromCollection = shop.get("name").toString();
            if (shopFromCollection.equals(shopName)) {
                System.out.println("Магазин уже есть в списке");
                return;
            }
        }

        Document shopDocument = new Document()
                .append("name", shopName);
        shopCollection.insertOne(shopDocument);
        System.out.println("Магазин успешно добавлен");

    }

    public static void addGood(String console, MongoCollection<Document> goodsCollection) {

        String[] split = console.split("\\s", 3);
        String goodName = split[1];
        int goodPrice = Integer.parseInt(split[2]);

        FindIterable<Document> goods = goodsCollection.find();
        for (Document good : goods) {
            String goodNameFromCollection = good.get("name").toString();
            if (goodNameFromCollection.equals(goodName)) {
                System.out.println("Товар уже есть в списке");
                return;
            }
        }


        Document goodDocument = new Document()
                .append("name", goodName)
                .append("price", goodPrice);

        goodsCollection.insertOne(goodDocument);
        System.out.println("Продукт успешно добавлен");

    }

    public static void showGood(String console,
                                MongoCollection<Document> goodsCollection,
                                MongoCollection<Document> shopCollection) {

        FindIterable<Document> goods = goodsCollection.find();
        FindIterable<Document> shops = shopCollection.find();


        String[] split = console.split("\\s", 3);
        String goodName = split[1];
        String shopName = split[2];

        String stringToCompare = "";

        for (Document goodDoc : goods) {
            String good = goodDoc.get("name").toString();
            if (good.equalsIgnoreCase(goodName)) {
                stringToCompare = good;
                break;
            }
        }

        if (!stringToCompare.equalsIgnoreCase(goodName)) { // проверяем, есть ли такой товар в базе товаров
            System.out.println("Продукт еще не существует, его нельзя выставить в магазине");
            return;
        }

        for (Document shopDoc : shops) {
            String shop = shopDoc.get("name").toString();
            if (shop.equalsIgnoreCase(shopName)) {
                stringToCompare = shop;
                break;
            }
        }
        if (!stringToCompare.equalsIgnoreCase(shopName)) { // проверяем, есть ли такой магазин в списке магазинов
            System.out.println("Магазин еще не существует, товар нельзя выставить");
            return;
        }


        Document goodDocument = new Document()
                .append("goodName", goodName);

        Bson filter = Filters.eq("name", shopName);
        Document query = new Document("$push", goodDocument);
        shopCollection.updateOne(filter, query);

        System.out.println("Продукт успешно выставлен");


    }

    public static void printStats(MongoCollection<Document> shopCollection,
                                  MongoCollection<Document> goodsCollection) {

        if (shopCollection.countDocuments() == 0 || goodsCollection.countDocuments() == 0) {
            System.out.println("Список пока пуст");
            return;
        }


        AggregateIterable<Document> aggregateResult = group(shopCollection);

        for (Document document : aggregateResult) {

            String shopName = document.get("_id").toString();
            shopName = shopName.substring(shopName.indexOf("=") + 1, shopName.indexOf("}"));

            double avgPrice = Double.parseDouble(document.get("avgprice").toString());
            int maxPrice = Integer.parseInt(document.get("maxprice").toString());
            int minPrice = Integer.parseInt(document.get("minprice").toString());
            int amountOfGoods = Integer.parseInt(document.get("total_goods_count").toString());
            int amountOfGoodsLessThan100 = Integer.parseInt(document.get("lt100").toString());


            System.out.printf("""
                            Магазин: %s
                            Всего товаров: %d
                            Средняя цена товаров: %.2f, Максимальная цена товара: %d, Минимальная цена товара: %d, \
                            Товаров меньше 100 рублей: %d
                            
                            """,
                    shopName, amountOfGoods, avgPrice, maxPrice, minPrice, amountOfGoodsLessThan100);

        }


    }

    public static void printShopsList(MongoCollection<Document> shopCollection) {
        if (shopCollection.countDocuments() == 0) {
            System.out.println("Список магазинов пока пуст");
            return;
        }
        FindIterable<Document> shopsIterable = shopCollection.find();
        System.out.println("Доступные магазины");
        for (Document doc : shopsIterable) {
            System.out.println(doc.get("name"));
        }
    }

    public static void printGoodsList(MongoCollection<Document> goodsCollection) {
        if (goodsCollection.countDocuments() == 0) {
            System.out.println("Список товаров пока пуст");
            return;
        }
        FindIterable<Document> goodsIterable = goodsCollection.find();
        System.out.println("Доступные товары");
        for (Document doc : goodsIterable) {
            System.out.println(doc.get("name"));
        }
    }

    private static AggregateIterable<Document> group(MongoCollection<Document> shopCollection) {
        Bson lookup = Aggregates.lookup("goods", "goodName", "name", "goods_list");
        Bson unwind = Aggregates.unwind("$goods_list");

        Bson group = BsonDocument.parse("{$group :  {_id : {name: \"$name\"}, " +
                "avgprice: {$avg: \"$goods_list.price\"}, " +
                "minprice: {$min: \"$goods_list.price\"}, " +
                "maxprice: {$max: \"$goods_list.price\"}, " +
                "total_goods_count: {$sum: 1}, " +
                "lt100:  {$sum: {$cond:  {if: {$lt: [\"$goods_list.price\", 100]}, then: 1, else: 0} }  }  } }");


        List<Bson> filters = new ArrayList<>();
        filters.add(lookup);
        filters.add(unwind);
        filters.add(group);


        return shopCollection.aggregate(filters);
    }


}
