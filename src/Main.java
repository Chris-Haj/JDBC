import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.*;

public class Main {
    static boolean running = true;
    static int load = 0;
    static Set<String> languagesList = new HashSet<>();

    public static void main(String[] args) throws FileNotFoundException, SQLException {
        start();
    }
/*This function will print the list of languages that are in the file and ask the user to choose one or more of them
* after the user enters the list of languages, the function will print all users that know all the languages that the user entered in.*/
    public static void Language(Connection con) throws SQLException {

        System.out.println("Please enter one or more languages seperated by spaces from this list:\n"+languagesList);
        Scanner scanner = new Scanner(System.in);
        String languages = scanner.nextLine();
        String[] LanguagesList = languages.split(" ");
        String query = "select s.Employment, s.Country, s.AgeStart, s.AgeEnd from StackOverFlow s, Languages l where s.Idx = l.Idx and l.language = ";
        String intersection = query;
        //This will create a select query for each language and intersect all of them with each-other to return
        // only people who know all the languages entered.
        for (int i = 0; i < LanguagesList.length; i++) {
            intersection += "'" + LanguagesList[i] + "'";
            if (i != LanguagesList.length - 1)
                intersection += " intersect " + query;
        }
        intersection += ";";
        Statement statement = con.createStatement();
        ResultSet res = statement.executeQuery(intersection);
        while (res.next()) {
            System.out.println(res.getString("Employment") + "\t" + res.getString("Country") + "\t" + res.getString("AgeStart") +
                    "-" + res.getString("AgeEnd"));
        }
        statement.close();
    }
/*
* This function loads in data by reading the csv file and create a table made of 11 columns with the same head titles as the csv file
* except the age will be split into the starting and ending range*/
    public static void Load(Connection con) throws FileNotFoundException, SQLException {
        System.out.println("Data is loading... May take some time.");
        String fileName = "StackDataBase.csv";
        File f = new File(fileName);
        String table = "CREATE TABLE StackOverFlowQuestionnaire (Idx INTEGER PRIMARY KEY, 'MainBranch' text,'Employment' text,'Country' text, 'Age1st' text,'LearnCode' text," +
                " 'YearsCode' text,'LanguagesWorkedWith' text, 'AgeStart' INTEGER, 'AgeEnd' INTEGER, 'Gender' text);";
        String insert = "insert into StackOverFlowQuestionnaire values (?,?,?,?,?,?,?,?,?,?,?)";
        /*This table will hold which languages each person holds.*/
        String table2 = """
                create table KnownLanguages(
                    Idx Integer,
                    Language text,
                    foreign key (Idx) references StackOverFlowQuestionnaire
                );
                """;
        String insert2 = "insert into KnownLanguages values(?, ?);";
        Statement st = con.createStatement();
        st.executeUpdate(table);
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
            for (int i = 0; i < sep.length; i++)
                line[i] = sep[i];
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
            if (languages.length == 0) {
                language.setInt(1, index);
                language.setString(2, "");
                language.executeUpdate();
            } else {
                for (String i : languages) {
                    language.setInt(1, index);
                    language.setString(2, i);
                    language.executeUpdate();
                    languagesList.add(i);
                }
            }
            execute.addBatch();
            batch++;
            if (batch % 300 == 0) {
                execute.executeBatch();
                System.out.print("Loading data... " + batch / 30 + "%\r");
            }
        }

        scanner.close();
        execute.close();
        language.close();
    }

    /*
     * Function to start the program called by main.*/
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

    /* Function to create a connection to a database and return the connection
     for it to be used by other functions
     to access the data base.*/
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

    /*Function to keep printing menu and taking input from the user.
     * Depending on what the user enters, the appropriate function is called.*/
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
                Special(con);
                break;
            case 5:
                running = false;
                break;
            default:
                System.out.println("Please enter a number from 1 to 5 only!");
        }
    }
/*This function asks to input an age range and a country name(optional)
* all users within this age range will be printed and if a country name was entered then it only print people
* that are within this age range and in the entered country.*/
    public static void Age(Connection con) throws SQLException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please enter the first age for the beginning of the range");
        int left = scanner.nextInt();
        System.out.println("Please enter the second age for the ending of the range");
        int right = scanner.nextInt();
        System.out.println("Please enter a country name or press enter without typing any country name");
        scanner.nextLine();
        String country = scanner.nextLine();
        boolean WithCountry = !country.equals("");
        String query = "select count(Idx) from StackOverFlowQuestionnaire where AgeStart<=" + left + " and AgeEnd>=" + right;
        query += WithCountry ? " and Country='" + country + "';" : ";";
        Statement statement = con.createStatement();
        ResultSet res = statement.executeQuery(query);
        System.out.print("Number of people between " + left + " to " + right + " is " + res.getString(1));
        if (WithCountry) {
            System.out.println(" In " + country);
        } else
            System.out.println("");
        statement.close();
        res.close();
    }

    /*This function will return all details about the people that live in the US and know a maximum of 2 languages*/
    public static void Special(Connection con) throws SQLException {
        String query = """
                select sof.*
                from StackOverFlowQuestionnaire sof, KnownLanguages kl
                where Country ='United States of America' and kl.Idx = sof.Idx
                group by sof.Idx
                having count(kl.Idx)<=2;
                """;
        Statement statement = con.createStatement();
        ResultSet res = statement.executeQuery(query);
        while (res.next()) {
            for (int i = 0; i < 11; i++) {
                System.out.print(res.getString(i + 1) + " ");
            }
            System.out.println("");
        }
        statement.close();
        res.close();
    }
}
