import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {

    private static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {

        System.out.println("Доступные команды: "
                + "\naddshop \"Название магазина\" - для добавления магазина"
                + "\naddgood \"Название товара\" - для добавления товара"
                + "\nshowgood \"Название товара\" \"Название магазина\" - для выставления товара в магазине"
                + "\nstats - для просмотра статистики"
                + "\nshopslist - для просмотра списка магазинов"
                + "\ngoodslist - для просмотра списка товаров"
                + "\nstop - для остановки программы");


        MongoClient mongoClient = new MongoClient();

        MongoDatabase testDB = mongoClient.getDatabase("test");

        MongoCollection<Document> shopCollection = testDB.getCollection("shops");
        MongoCollection<Document> goodsCollection = testDB.getCollection("goods");

        System.out.println("Введите команду: ");

        while (true) {
            String console = scanner.nextLine();

            /*
            Добавляем магазин
             */

            if (shopMatcher(console)) {

                String[] split = console.split("\\s", 2);
                String shopName = split[1];

                Document shopDocument = new Document()
                        .append("name", shopName);
                shopCollection.insertOne(shopDocument);
                System.out.println("Магазин успешно добавлен");

                /*
                 * Добавляем товар
                 * */

            } else if (goodsMatcher(console)) {

                String[] split = console.split("\\s", 3);
                String goodName = split[1];
                int goodPrice = Integer.parseInt(split[2]);

                Document goodDocument = new Document()
                        .append("name", goodName)
                        .append("price", goodPrice);

                goodsCollection.insertOne(goodDocument);
                System.out.println("Продукт успешно добавлен");

                /*

                 * Выставляем товар
                 */


            } else if (showGoodsMatcher(console)) {

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
                    continue;
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
                    continue;
                }


                Document goodDocument = new Document()
                        .append("goodName", goodName);

                Bson filter = Filters.eq("name", shopName);
                Document query = new Document("$push", goodDocument);
                shopCollection.updateOne(filter, query);

                System.out.println("Продукт успешно выставлен");

                /*
                 * Выводим статистику по магазинам
                 * */

            } else if (console.equalsIgnoreCase("stats")) {


                if (shopCollection.countDocuments() == 0 || goodsCollection.countDocuments() == 0) {
                    System.out.println("Список пока пуст");
                    continue;
                }


                AggregateIterable<Document> aggregateResult = group(shopCollection);

                for (Document document : aggregateResult) {
                    String shopName = document.get("_id").toString();

                    double avgPrice = Double.parseDouble(document.get("avgprice").toString());
                    int maxPrice = Integer.parseInt(document.get("maxprice").toString());
                    int minPrice = Integer.parseInt(document.get("minprice").toString());

                    int amountOfGoods = 0;

                    List<Bson> projectList = new ArrayList<>();
                    Bson project = Aggregates.project(BsonDocument.parse("{   name : 1,    numberOfGoods :  " +
                            "{$cond : {if: {$isArray : \"$goodName\"}, then : {$size: \"$goodName\"}, else : \"0\"}} }"));
                    projectList.add(project);

                    AggregateIterable<Document> aggregate = shopCollection.aggregate(projectList);
                    for (Document doc : aggregate) {
                        if (doc.get("name").toString().equalsIgnoreCase(shopName)) {
                            amountOfGoods = Integer.parseInt(doc.get("numberOfGoods").toString());
                            break;
                        }
                    }

                    System.out.printf("Магазин: %s\nВсего товаров: %d\nСредняя цена товаров %.2f, " +
                                    "Максимальная цена товара %d, Минимальная цена товара %d\n",
                            shopName, amountOfGoods, avgPrice, maxPrice, minPrice);

                }

                /*
                Выводим список магазинов
                 */

            } else if (console.equalsIgnoreCase("shopslist")) {

                if (shopCollection.countDocuments() == 0) {
                    System.out.println("Список магазинов пока пуст");
                    continue;
                }
                FindIterable<Document> shopsIterable = shopCollection.find();
                System.out.println("Доступные магазины");
                for (Document doc : shopsIterable) {
                    System.out.println(doc.get("name"));
                }

                /*
                Выводим список товаров
                 */

            } else if (console.equalsIgnoreCase("goodslist")) {
                if (goodsCollection.countDocuments() == 0) {
                    System.out.println("Список товаров пока пуст");
                    continue;
                }
                FindIterable<Document> goodsIterable = goodsCollection.find();
                System.out.println("Доступные товары");
                for (Document doc : goodsIterable) {
                    System.out.println(doc.get("name"));
                }

                /*
                 * Останавливаем программу
                 *
                 * */

            } else if (console.equalsIgnoreCase("stop")) {
                break;

            } else {
                System.out.println("Неверная команда");
            }
        }

        scanner.close();


    }

    private static boolean shopMatcher(String console) {
        return console.matches("(a?A?d{2}s?S?hop)\\s\\S+");
    }

    private static boolean goodsMatcher(String console) {
        return console.matches("(a?A?ddg?G?ood)\\s([A-Za-z]*)?([А-Яа-я]*)?\\s\\d+");
    }

    private static boolean showGoodsMatcher(String console) {
        return console.matches("(s?S?howg?G?ood)\\s[A-я]+\\s[A-я]+");
    }


    private static AggregateIterable<Document> group(MongoCollection<Document> shopCollection) {
        Bson lookup = Aggregates.lookup("goods", "goodName", "name", "goods_list");
        Bson unwind = Aggregates.unwind("$goods_list");

        Bson group = Aggregates.group("$name", Accumulators.avg("avgprice", "$goods_list.price"),
                Accumulators.max("maxprice", "$goods_list.price"),
                Accumulators.min("minprice", "$goods_list.price"));


        List<Bson> filters = new ArrayList<>();
        filters.add(lookup);
        filters.add(unwind);
        filters.add(group);


        AggregateIterable<Document> aggregate = shopCollection.aggregate(filters);
        return aggregate;
    }
}
