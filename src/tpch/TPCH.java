/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tpch;

import connection.ConnectionFactory;
import java.io.IOException;
import java.sql.Connection;
import queries.TPCHQueriesTime;

/**
 *
 * @author LTorres
 */
public class TPCH {

    // mysql, monetdb, postgresql
    static String SGBD = "monetdb";
    static String DB = "tpch1";
    static String user = "monetdb";
    static String password = "monetdb";

    public static void main(String[] args) throws IOException {

        ConnectionFactory cf = new ConnectionFactory(SGBD, DB, user, password);
        Connection con = cf.getConnection();
        System.out.println("Connected to " + SGBD + " - " + DB);

        TPCHQueriesTime tpchQT = new TPCHQueriesTime();
        tpchQT.getTpchq().setConnection(con);
        tpchQT.run();
        tpchQT.saveQueriesTime("queries_time.txt");
    }
}
