import Core.MongoCommand;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Scanner;

public class App {

    static {
        System.out.println(
                "Доступные команды: "
                + "\naddshop \"Название магазина\" - для добавления магазина"
                + "\naddgood \"Название товара\" - для добавления товара"
                + "\nshowgood \"Название товара\" \"Название магазина\" - для выставления товара в магазине"
                + "\nstats - для просмотра статистики"
                + "\nshopslist - для просмотра списка магазинов"
                + "\ngoodslist - для просмотра списка товаров"
                + "\nstop - для остановки программы");

    }

    private static final Scanner scanner = new Scanner(System.in);


    public static void main(String[] args) {


        MongoClient mongoClient = new MongoClient("localhost", 27017);

        MongoDatabase testDB = mongoClient.getDatabase("test");

        MongoCollection<Document> shopCollection = testDB.getCollection("shops");
        MongoCollection<Document> goodsCollection = testDB.getCollection("goods");

        System.out.println("Введите команду: ");

        while (true) {

            String console = scanner.nextLine();

            if (shopMatcher(console)) { // добавляем магазин

                MongoCommand.addShop(console, shopCollection);

            } else if (goodsMatcher(console)) { // добавляем товар

                MongoCommand.addGood(console, goodsCollection);

            } else if (showGoodsMatcher(console)) { // добавление товара в магазин

                MongoCommand.showGood(console, goodsCollection, shopCollection);

            } else if (console.equalsIgnoreCase("stats")) { // вывод статистики магазинов

                MongoCommand.printStats(shopCollection, goodsCollection);

            } else if (console.equalsIgnoreCase("shopslist")) { // вывод списка магазинов

                MongoCommand.printShopsList(shopCollection);

            } else if (console.equalsIgnoreCase("goodslist")) { // вывод списка товаров

                MongoCommand.printGoodsList(goodsCollection);

            } else if (console.equalsIgnoreCase("stop")) { // остановка программы

                break;

            } else {

                System.out.println("Неверная команда");

            }
        }

        scanner.close();
        mongoClient.close();


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

}
