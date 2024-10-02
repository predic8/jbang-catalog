///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS info.picocli:picocli:4.7.6
//DEPS com.h2database:h2:2.3.232
//DEPS org.postgresql:postgresql:42.7.4

import picocli.CommandLine.*;

import java.sql.*;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;

@Command(name = "sql", mixinStandardHelpOptions = true, version = "sql 0.1", description = "sql with jbang")
class sql2 implements Callable<Integer> {

    @Parameters(index = "0", description = "SQL statement")
    String statement;

    @Option(names = { "-u", "--url" }, description = "JDBC URL")
    String url = "jdbc:h2:~/default";

    @Option(names = { "-l", "--login" }, description = "Login user")
    String user = "";

    @Option(names = { "-p", "--password" }, description = "password")
    String password = "";

    public static void main(String... args) {
        System.exit(new CommandLine(new sql2()).execute(args));
    }

    @Override
    public Integer call() throws Exception {

        Connection con = DriverManager.getConnection(url, user, password);

        if (statement.toLowerCase().contains("select ")) {
            query(con);
        } else {
            execute(con);
        }

        con.close();
        return 0;
    }

    int execute(Connection con) {
        try (Statement st = con.createStatement()) {
            st.execute(statement);
        } catch (SQLException e) {
            System.out.println(e);
        }
        return 0;
    }

    int query(Connection con) {
        try (Statement st = con.createStatement()) {
            ResultSet rs = st.executeQuery(statement);

            int columns = rs.getMetaData().getColumnCount();

            while (rs.next()) {
                for (int i = 1; i <= columns; i++) {
                    System.out.print(rs.getObject(i));
                    if (i != columns)
                        System.out.print(" | ");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            System.out.println(e);
        }

        return 0;
    }
}
