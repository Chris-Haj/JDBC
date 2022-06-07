import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;


public class Main {
    public static final String RED = "\u001B[31m";
    public static final String RESET = "\u001B[0m";
    static boolean running = true;
    public static int load = 0;

    public static void main(String[] args) throws FileNotFoundException, SQLException {
        Connection con = connect();
        con.close();
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
        Connection c = null;
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
                    System.out.println(RED + "Cannot load data more than once!" + RESET);
                else {
                    Load(con);
                }
                break;
            case 2:

                break;
            case 3:
                break;
            case 4:
                break;
            case 5:
                running = false;
                break;
            default:
                System.out.println("Please enter a number from 1 to 6 only!");

        }
    }

    public static void Load(Connection con) throws FileNotFoundException, SQLException {
        String fileName = "/home/chris/IdeaProjects/DataBases/src/StackDataBase.csv";
        File f = new File(fileName);
        Scanner scanner = new Scanner(f);
        ArrayList<String[]> Lines = new ArrayList<>();
        String curLine;
        while (scanner.hasNextLine()) {
            curLine = scanner.nextLine();
            String[] sep = curLine.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            Lines.add(sep);
        }
        String[] headers = Lines.get(0);
        String table = ("CREATE TABLE StackOverFlowQuestionnaire " +
                "(Idx INTEGER PRIMARY KEY, %s text,%s text, %s text, %s text,%s text, %s text,%s text, %s text);").formatted(headers[1], headers[2],
                headers[3], headers[4], headers[5],
                headers[6], headers[7], headers[8]);
        Statement execute = con.createStatement();
        System.out.println(table);
        execute.executeUpdate(table);
        String sql;
        int k=0;
        for (String[] i : Lines) {
            if(k++==0)
                continue;
            headers = i;
            sql = "INSERT INTO StackOverFlowQuestionnaire values(%s,'%s','%s','%s','%s','%s','%s','%s','%s')".formatted(headers[0], headers[1], headers[2], headers[3], headers[4], headers[5],
                    headers[6], headers[7], headers[8]);
            System.out.println(sql);
            execute.executeUpdate(sql);
        }
        execute.close();

    }

}
