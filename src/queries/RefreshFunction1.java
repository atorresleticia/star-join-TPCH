/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queries;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author LTorres
 */
public class RefreshFunction1 {

    private Connection con;

    public RefreshFunction1() {
    }

    public RefreshFunction1(Connection con) {
        this.con = con;
    }

    public Connection getConnection() {
        return con;
    }

    public void setConnection(Connection con) {
        this.con = con;
    }

    public void update(int streamNo) {
        Statement st = null;
        try {
            st = this.con.createStatement();
            String sql = String.format("INSERT INTO item (i_itemkey, i_partkey, i_suppkey, i_custkey, i_linenumber, i_quantity,\n"
                    + "        i_extendedprice, i_discount, i_tax, i_returnflag, i_linestatus,\n"
                    + "        i_shipdate, i_commitdate, i_receiptdate, i_shipinstruct, i_shipmode,\n"
                    + "        i_orderstatus, i_ordertotalprice, i_orderdate, i_orderpriority,\n"
                    + "        i_shippriority, i_ordercomment) "
                    + "SELECT i_itemkey, i_partkey, i_suppkey, i_custkey, i_linenumber, i_quantity,\n"
                    + "        i_extendedprice, i_discount, i_tax, i_returnflag, i_linestatus,\n"
                    + "        i_shipdate, i_commitdate, i_receiptdate, i_shipinstruct, i_shipmode,\n"
                    + "        i_orderstatus, i_ordertotalprice, i_orderdate, i_orderpriority,\n"
                    + "        i_shippriority, i_ordercomment FROM rf1.item%d", streamNo);

            st.executeUpdate(sql);
        } catch (SQLException ex) {
            Logger.getLogger(RefreshFunction1.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                st.close();
            } catch (SQLException ex) {
                Logger.getLogger(RefreshFunction1.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
