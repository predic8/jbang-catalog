///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS info.picocli:picocli:4.7.6
//DEPS com.h2database:h2:2.3.232
//DEPS org.postgresql:postgresql:42.7.4

import picocli.*;
import picocli.CommandLine.*;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

@Command(name = "sql", mixinStandardHelpOptions = true, version = "sql 0.1", description = "sql with jbang")
class sql implements Callable<Integer> {

    @Parameters(index = "0", description = "SQL statement", defaultValue = "")
    String statement;

    @Option(names = { "-u", "--url" }, description = "JDBC URL")
    String url = "jdbc:h2:~/default";

    @Option(names = { "-l", "--login" }, description = "Login user")
    String user = "";

    @Option(names = { "-p", "--password" }, description = "password")
    String password = "";

    @Option(names = { "-s", "--stdin" }, description = "Read from stdin")
    boolean stdin;

    Connection con;

    public static void main(String... args) {
        System.exit(new CommandLine(new sql()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        con = DriverManager.getConnection(url, user, password);
        if (stdin) {
            processStdin();
        } else {
            if (statement.isEmpty()) {
                CommandLine.usage(this, System.out);
            }
            execute(statement);
        }
        con.close();
        return 0;
    }

    void processStdin() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String line;
        while ((line = reader.readLine()) != null) {
            execute(line);
        }
    }

    void execute(String sql) {
        try (Statement st = con.createStatement()) {
            boolean resultAvailable = st.execute(sql);
            if (!resultAvailable) {
                return;
            }
            ResultSet rs = st.getResultSet();
            printResultSet(readResult(rs), getColumnNames(rs));
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    static void printResultSet(List<List<String>> result, List<String> columnNames) {
        List<Integer> maxLengths = getMaxColumnLengths(result, columnNames);

        for (int i = 0; i < columnNames.size(); i++) {
            System.out.print(fill(columnNames.get(i),maxLengths.get(i)));
            System.out.print(" | ");
        }

        System.out.println();

        for (int i = 0; i < columnNames.size(); i++) {
            System.out.print("-".repeat(maxLengths.get(i)));
            System.out.print("-|");
            if (i < columnNames.size() - 1) {
                System.out.print("-");
            }
        }

        System.out.println();

        result.forEach(row -> {
            for (int i = 0; i < row.size(); i++) {
                System.out.print(fill(row.get(i), maxLengths.get(i)));
                System.out.print(" | ");
            }
            System.out.println();
        });
    }

    static List<String> getColumnNames(ResultSet rs) throws SQLException {
        List<String> columnNames = new ArrayList<>();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            columnNames.add(rs.getMetaData().getColumnName((i)));
        }
        return columnNames;
    }

    static List<List<String>> readResult(ResultSet rs) throws SQLException {
        List<List<String>> result;
        result = new ArrayList<>();

        while (rs.next()) {
            List<String> row = new ArrayList<>();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                String value = rs.getString(i);
                row.add(value == null ? "NULL" : value);
            }
            result.add(row);
        }
        return result;
    }

    static List<Integer> getMaxColumnLengths(List<List<String>> result, List<String> columnNames) {

        List<List<String>> data = new ArrayList<>(result);
        data.add(columnNames);

        List<Integer> maxLengths = new ArrayList<>();

        // Initialize maxLengths with zeros
        for (int i = 0; i < data.getFirst().size(); i++) {
            maxLengths.add(0);
        }

        // Iterate over each row
        for (List<String> row : data) {
            for (int i = 0; i < row.size(); i++) {
                int currentLength = row.get(i).length();
                if (currentLength > maxLengths.get(i)) {
                    maxLengths.set(i, currentLength);
                }
            }
        }

        return maxLengths;
    }

    static String fill(String s, int characters) {
        // Check if the string is already longer than the desired length
        if (s.length() >= characters) {
            return s;
        }

        // Return the new string with spaces prepended
        return " ".repeat(characters - s.length()) + s;
    }
}