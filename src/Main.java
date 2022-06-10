import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.*;

public class Main {

    static boolean running = true;
    public static int load = 0;
    public static String[] Titles;

    public static void main(String[] args) throws FileNotFoundException, SQLException {
        createLangsTable(connect());
    }
    private static void start() throws FileNotFoundException, SQLException {
        Connection con = connect();
        while (running) {
            PrintMenu(con);
        }
        try {
            if (con != null)
                con.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Program exiting...");
    }

    public static Connection connect() {
        Connection c;
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:StackOverFlow.db");
            System.out.println("Opened database successfully");
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
        return c;
    }

    public static void PrintMenu(Connection con) throws FileNotFoundException, SQLException {
        Scanner input = new Scanner(System.in);
        System.out.println("What do you want to do?\n" +
                "1. Load Data\n" +
                "2. Language\n" +
                "3. Age\n" +
                "4. Special\n" +
                "5. Exit");
        int option = input.nextInt();
        switch (option) {
            case 1:
                if (load++ >= 1)
                    System.err.println("Cannot load data more than once!");
                else {
                    System.out.println("Please wait while the data is loaded...");
                    Load(con);
                    System.out.println("Finished loading");
                }
                break;
            case 2:
                Language(con);
                break;
            case 3:
                Age(con);
                break;
            case 4:
                break;
            case 5:
                running = false;
                break;
            default:
                System.out.println("Please enter a number from 1 to 5 only!");
        }
    }

    public static void Age(Connection con) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please enter the first age for the beginning of the range");
        int left = scanner.nextInt();
        System.out.println("Please enter the second age for the ending of the range");
        int right = scanner.nextInt();
        System.out.println("Please enter a country name or press enter without typing any country name");
        String country = scanner.nextLine();

    }

    public static void Language(Connection con) throws SQLException{

        System.out.println("Please enter a language or languages seperated by ';' from this list");

        Scanner scanner = new Scanner(System.in);
        String language = scanner.nextLine();
        String select = "select Employment, Country,Age from StackOverFlowQuestionnaire" +
                " where '%s'==LanguageHaveWorkedWith".formatted(language);
        Statement execute = con.createStatement();
        ResultSet res = execute.executeQuery(select);
        while(res.next()){
            System.out.println(res.getString("Employment")+"\t"+res.getString("Country")+"\t"+res.getString("Age"));
        }

    }
    public static void createLangsTable(Connection con){
        try {
            Set<String> langs = ListOfLanguages(con);
            String table = "create table languages(ID INTEGER PRIMARY KEY,text Language)";
            String insert = "insert into languages values(%d, '%s');";
            Statement execute = con.createStatement();
            execute.executeUpdate(table);
            int i=0;
            for(String language : langs){
                execute.executeUpdate(insert.formatted(i++,language));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public static Set<String> ListOfLanguages(Connection con) throws SQLException{
        ResultSet res;
        String select = "select distinct LanguageHaveWorkedWith from StackOverFlowQuestionnaire";
        Statement execute = con.createStatement();
        res = execute.executeQuery(select);
        Set<String> langs = new HashSet<>();
        while(res.next()){
            String[] l = res.getString("LanguageHaveWorkedWith").split(";");
            for(String i : l)
                langs.add(i);
        }

        execute.close();
        return langs;

    }
    public static void Load(Connection con) throws FileNotFoundException, SQLException {
        String fileName = "StackDataBase.csv";
        File f = new File(fileName);
        Scanner scanner = new Scanner(f);
        ArrayList<String[]> Lines = new ArrayList<>();
        String curLine;
        while (scanner.hasNextLine()) {
            curLine = scanner.nextLine();
            String[] sep = curLine.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            Lines.add(sep);
        }
        String[] headers = Arrays.copyOfRange(Lines.get(0),0,Lines.get(0).length);
        Titles = Arrays.copyOfRange(Lines.get(0),0,Lines.get(0).length);
        String table = ("CREATE TABLE StackOverFlowQuestionnaire (Idx INTEGER PRIMARY KEY, %s text,%%s text, %%s text, %%s text,%%s text, %%s text,%%s text, %%s text, %%s text);".formatted()).formatted(headers[1], headers[2],
                headers[3], headers[4], headers[5],
                headers[6], headers[7], headers[8],headers[9]);
        Statement execute = con.createStatement();
        execute.executeUpdate(table);
        String insert =  "INSERT INTO StackOverFlowQuestionnaire values(%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s')";
        String curInsert;
        int k=0;
        for (String[] line : Lines) {
            if (k++ == 0)
                continue;
            headers = new String[Lines.get(0).length];
            int j = 0;
            for (; j < line.length; j++)
                headers[j] = line[j].replace("'","''");
            while (j < Lines.get(0).length)
                headers[j++] = "";
            curInsert = insert.formatted(headers[0], headers[1], headers[2], headers[3], headers[4], headers[5],
                    headers[6], headers[7], headers[8], headers[9]);
            execute.executeUpdate(curInsert);
        }
        execute.close();
    }
}
