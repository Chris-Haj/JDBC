import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.*;

public class Main {
    static boolean running = true;
    static int load = 0;
    public static void Language(Connection con) throws SQLException {

        System.out.println("Please enter a language or languages seperated by ';' from this list");

        Scanner scanner = new Scanner(System.in);
        String language = scanner.nextLine();
        String select = "select Employment, Country,Age from StackOverFlowQuestionnaire" +
                " where '%s'==LanguageHaveWorkedWith".formatted(language);
        Statement execute = con.createStatement();
        ResultSet res = execute.executeQuery(select);
        while (res.next()) {
            System.out.println(res.getString("Employment") + "\t" + res.getString("Country") + "\t" + res.getString("Age"));
        }

    }

    public static Set<String> ListOfLanguages(Connection con) throws SQLException {
        ResultSet res;
        String select = "select distinct LanguageHaveWorkedWith from StackOverFlowQuestionnaire";
        Statement execute = con.createStatement();
        res = execute.executeQuery(select);
        Set<String> langs = new HashSet<>();
        while (res.next()) {
            String[] l = res.getString("LanguageHaveWorkedWith").split(";");
            for (String i : l)
                langs.add(i);
        }

        execute.close();
        return langs;

    }



    public static void createLangsTable(Connection con) {
        try {
            Set<String> langs = ListOfLanguages(con);
            String table = """
                    create table langs(
                        Idx Integer,
                        Language text,
                        foreign key (Idx) references StackOverFlowQuestionnaire
                    );
                    """;
            String insert = "insert into KnownLanguages values(?, ?);";
            Statement execute = con.createStatement();
            execute.executeUpdate(table);
            int i = 0;
            for (String language : langs) {
                execute.executeUpdate(insert.formatted(i++, language));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public static void main(String[] args) throws FileNotFoundException, SQLException {
//        Load(connect());
        Scanner scanner = new Scanner(new File("StackDataBase.csv"));
        while(scanner.hasNext()){
            System.out.println(scanner.nextLine());
        }
    }

    public static void Load(Connection con) throws FileNotFoundException, SQLException {
        String fileName = "StackDataBase.csv";
        File f = new File(fileName);
        String table = "CREATE TABLE StackOverFlowQuestionnaire (Idx INTEGER PRIMARY KEY, 'MainBranch' text,'Employment' text,'Country' text, 'Age1st' text,'LearnCode' text," +
                " 'YearsCode' text,'LanguagesWorkedWith' text, 'AgeStart' INTEGER, 'AgeEnd' INTEGER, 'Gender' text);";
        String insert = "insert into StackOverFlowQuestionnaire values (?,?,?,?,?,?,?,?,?,?,?)";
        String table2= """
                    create table KnownLanguages(
                        Idx Integer,
                        Language text,
                        foreign key (Idx) references StackOverFlowQuestionnaire
                    );
                    """;
        String insert2 = "insert into KnownLanguages values(?, ?);";
        Statement st = con.createStatement();
//        st.executeUpdate(table);
        st.executeUpdate(table2);
        st.close();
        PreparedStatement execute = con.prepareStatement(insert);
        PreparedStatement language = con.prepareStatement(insert2);
        Scanner scanner = new Scanner(f);
        String curLine;
        String[] line = new String[11];
        int batch = 0;
        String ageStart = "", ageEnd = "";
        scanner.nextLine();
        int index;
        while (scanner.hasNextLine()) {
            curLine = scanner.nextLine();
            String[] sep = curLine.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            index = Integer.parseInt(sep[0]);
            if (sep.length >= 9) {
                if (sep[8].equals("") || sep[8].charAt(0) == 'P' || sep[8].length() == 0) {
                    ageStart = "0";
                    ageEnd = "0";
                } else if (sep[8].charAt(0) == 'U') {
                    ageStart = "0";
                    ageEnd = String.valueOf(Integer.parseInt(sep[8].replaceAll("[\\D]", "")));
                } else if (sep[8].charAt(0) == 'O') {
                    ageStart = String.valueOf(Integer.parseInt(sep[8].replaceAll("[\\D]", "")));
                    ageEnd = "100";
                } else if (sep[8].indexOf('-') != -1) {
                    String[] range = sep[8].split("-");
                    ageStart = String.valueOf(Integer.parseInt(range[0].replaceAll("[\\D]", "")));
                    ageEnd = String.valueOf(Integer.parseInt(range[1].replaceAll("[\\D]", "")));
                }
            }
            for (int i = 0; i < sep.length; i++) {
                line[i] = sep[i];
            }
            line[8] = ageStart;
            line[9] = ageEnd;
            line[10] = sep.length == 10 ? sep[9] : "";

            execute.setInt(1, index);
            for (int i = 1; i < 11; i++) {
                if (i == 8 || i == 9) {
                    if (line[i].equals(""))
                        line[i] = "0";
                    execute.setInt(i + 1, Integer.parseInt(line[i]));
                } else
                    execute.setString(i + 1, line[i]);
            }
            ageStart = "";
            ageEnd = "";
            String[] languages = line[7].split(";");
            if(languages.length==0){
                language.setInt(1,index);
                language.setString(2,"");
                language.executeUpdate();
            }
            else{
                for(String i : languages){
                    language.setInt(1,index);
                    language.setString(2,i);
                    language.executeUpdate();
                }
            }
//            execute.addBatch();
            batch++;
            if (batch % 100 == 0){
//                execute.executeBatch();
            }
        }
        scanner.close();
        execute.close();
        language.close();
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
        System.out.println("""
                What do you want to do?
                1. Load Data
                2. Language
                3. Age
                4. Special
                5. Exit""");
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
}
