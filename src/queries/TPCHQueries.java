/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queries;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author LTorres
 */
public class TPCHQueries {

    Connection con = null;

    public TPCHQueries() {
    }

    public TPCHQueries(Connection con) {
        this.con = con;
    }

    public Connection getConnection() {
        return con;
    }

    public void setConnection(Connection con) {
        this.con = con;
    }

    public void runQueries() {
//        query1();
        query2();
        query3();
        query4();
        query5();
        query6();
        query7();
        query8();
        query9();
        query10();
        query11();
        query12();
        query13();
        query14();
        query15();
        query16();
        query17();
        query18();
        query19();
        query20();
        query21();
        query22();
    }

    public void executeQuery(String sql) {

        Statement st = null;
        ResultSet rs = null;

        try {
            st = con.createStatement();
            rs = st.executeQuery(sql);
            //ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                /*for (int j = 1; j <= md.getColumnCount(); j++) {
                    System.out.print(rs.getString(j) + "\t");
                }
                System.out.println("");*/
            }
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueries.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            /*try {
                st.close();
                rs.close();
            } catch (SQLException ex) {
                Logger.getLogger(TPCHQueries.class.getName()).log(Level.SEVERE, null, ex);
            }*/
        }

    }

    public void query1() {

        String sql;
        sql = "select\n"
                + "        i_returnflag,\n"
                + "        i_linestatus,\n"
                + "        sum(i_quantity) as sum_qty,\n"
                + "        sum(i_extendedprice) as sum_base_price,\n"
                + "        sum(i_extendedprice * (1 - i_discount)) as sum_disc_price,\n"
                + "        sum(i_extendedprice * (1 - i_discount) * (1 + i_tax)) as sum_charge,\n"
                + "        avg(i_quantity) as avg_qty,\n"
                + "        avg(i_extendedprice) as avg_price,\n"
                + "        avg(i_discount) as avg_disc,\n"
                + "        count(*) as count_order\n"
                + "from\n"
                + "        item\n"
                + "where\n"
                + "        i_shipdate <= date '1998-12-01' - interval '90' day\n"
                + "group by\n"
                + "        i_returnflag,\n"
                + "        i_linestatus\n"
                + "order by\n"
                + "        i_returnflag,\n"
                + "        i_linestatus;";

        executeQuery(sql);
    }

    public void query2() {

        String sql;
        sql = "select distinct\n"
                + "	s_acctbal,\n"
                + "        s_name,\n"
                + "        s_nation_name,\n"
                + "        p_partkey,\n"
                + "	p_mfgr,\n"
                + "        s_address,\n"
                + "        s_phone,\n"
                + "        s_comment\n"
                + "from\n"
                + "        item,\n"
                + "        supplier,\n"
                + "        part\n"
                + "where\n"
                + "        i_partkey = p_partkey \n"
                + "        and i_suppkey = s_suppkey\n"
                + "        and p_size = 15\n"
                + "        and p_type like '%BRASS'\n"
                + "        and s_region_name = 'EUROPE'\n"
                + "        and i_supplycost = (\n"
                + "                select\n"
                + "                        min(i_supplycost)\n"
                + "                from\n"
                + "                        item,\n"
                + "                        supplier\n"
                + "                where\n"
                + "			i_partkey = p_partkey\n"
                + "                        and i_suppkey = s_suppkey \n"
                + "                        and s_region_name = 'EUROPE'\n"
                + "        )\n"
                + "order by\n"
                + "        s_acctbal desc,\n"
                + "        s_nation_name,\n"
                + "        s_name,\n"
                + "        p_partkey\n"
                + "	limit 100;";

        executeQuery(sql);
    }

    public void query3() {

        String sql;
        sql = "select\n"
                + "	i_itemkey,\n"
                + "	sum(i_extendedprice * (1 - i_discount)) as revenue,\n"
                + "	i_orderdate,\n"
                + "	i_shippriority\n"
                + "from\n"
                + "	customer,\n"
                + "	item\n"
                + "where\n"
                + "	c_mktsegment = 'BUILDING'\n"
                + "	and c_custkey = i_custkey\n"
                + "	and i_orderdate < date '1995-03-15'\n"
                + "	and i_shipdate > date '1995-03-15'\n"
                + "group by\n"
                + "	i_itemkey,\n"
                + "	i_orderdate,\n"
                + "	i_shippriority\n"
                + "order by\n"
                + "	revenue desc,\n"
                + "	i_orderdate\n"
                + "limit 10;";

        executeQuery(sql);
    }

    public void query4() {
        String sql;
        sql = "select\n"
                + "	i_orderpriority,\n"
                + "	count(*) as order_count\n"
                + "from\n"
                + "	item\n"
                + "where\n"
                + "	i_orderdate >= date '1993-07-01'\n"
                + "	and i_orderdate < date '1993-07-01' + interval '3' month\n"
                + "	and exists (\n"
                + "		select\n"
                + "			*\n"
                + "		from\n"
                + "			item\n"
                + "		where\n"
                + "			i_commitdate < i_receiptdate\n"
                + "	)\n"
                + "group by\n"
                + "	i_orderpriority\n"
                + "order by\n"
                + "	i_orderpriority;";
        executeQuery(sql);
    }

    public void query5() {
        String sql;
        sql = "select\n"
                + "	s_nation_name,\n"
                + "	sum(i_extendedprice * (1 - i_discount)) as revenue\n"
                + "from\n"
                + "	customer,\n"
                + "	item\n"
                + "	supplier,\n"
                + "where\n"
                + "	c_custkey = i_custkey\n"
                + "	and i_suppkey = s_suppkey\n"
                + "	and c_nationkey = s_nationkey\n"
                + "	and s_region_name = 'ASIA'\n"
                + "	and i_orderdate >= date '1994-01-01'\n"
                + "	and i_orderdate < date '1994-01-01' + interval '1' year\n"
                + "group by\n"
                + "	s_nation_name\n"
                + "order by\n"
                + "	revenue desc;";
        executeQuery(sql);
    }

    public void query6() {
        String sql;
        sql = "select\n"
                + "	sum(i_extendedprice * i_discount) as revenue\n"
                + "from\n"
                + "	item\n"
                + "where\n"
                + "	i_shipdate >= date '1994-01-01'\n"
                + "	and i_shipdate < date '1994-01-01' + interval '1' year\n"
                + "	and i_discount between y - 0.01 and y + 0.01\n"
                + "	and i_quantity < 24;";
        executeQuery(sql);
    }

    public void query7() {
        String sql;
        sql = "select\n"
                + "	supp_nation,\n"
                + "	cust_nation,\n"
                + "	i_year,\n"
                + "	sum(volume) as revenue\n"
                + "from\n"
                + "	(\n"
                + "		select\n"
                + "			s_nation_name as supp_nation,\n"
                + "			c_nation_name as cust_nation,\n"
                + "			extract(year from i_shipdate) as i_year,\n"
                + "			i_extendedprice * (1 - i_discount) as volume\n"
                + "		from\n"
                + "			supplier,\n"
                + "			item,\n"
                + "			customer,\n"
                + "		where\n"
                + "			s_suppkey = i_suppkey\n"
                + "			and c_custkey = i_custkey\n"
                + "			and (\n"
                + "				(s_nation_name = 'FRANCE' and c_nation_name = 'GERMANY')\n"
                + "				or (s_nation_name = 'GERMANY' and c_nation_name = 'FRANCE')\n"
                + "			)\n"
                + "			and i_shipdate between date '1995-01-01' and date '1996-12-31'\n"
                + "	) as shipping\n"
                + "group by\n"
                + "	supp_nation,\n"
                + "	cust_nation,\n"
                + "	i_year\n"
                + "order by\n"
                + "	supp_nation,\n"
                + "	cust_nation,\n"
                + "	i_year;";
        executeQuery(sql);
    }

    public void query8() {
        String sql;
        sql = "select\n"
                + "	order_year,\n"
                + "	sum(case\n"
                + "		when nation = 'BRAZIL' then volume\n"
                + "		else 0\n"
                + "	end) / sum(volume) as mkt_share\n"
                + "from\n"
                + "	(\n"
                + "		select\n"
                + "			extract(year from i_orderdate) as order_year,\n"
                + "			i_extendedprice * (1 - i_discount) as volume,\n"
                + "			s_nation_name as nation\n"
                + "		from\n"
                + "			part,\n"
                + "			supplier,\n"
                + "			item,\n"
                + "			customer\n"
                + "		where\n"
                + "			p_partkey = i_partkey\n"
                + "			and s_suppkey = i_suppkey\n"
                + "			and i_custkey = c_custkey\n"
                + "			and c_region_name = 'AMERICA'\n"
                + "			and i_orderdate between date '1995-01-01' and date '1996-12-31'\n"
                + "			and p_type = 'ECONOMY ANODIZED STEEL'\n"
                + "	) as all_nations\n"
                + "group by\n"
                + "	order_year\n"
                + "order by\n"
                + "	order_year;";
        executeQuery(sql);
    }

    public void query9() {
        String sql;
        sql = "select\n"
                + "	nation,\n"
                + "	order_year,\n"
                + "	sum(amount) as sum_profit\n"
                + "from\n"
                + "	(\n"
                + "		select\n"
                + "			s_nation_name as nation,\n"
                + "			extract(year from i_orderdate) as order_year,\n"
                + "			i_extendedprice * (1 - i_discount) - i_supplycost * i_quantity as amount\n"
                + "		from\n"
                + "			part,\n"
                + "			supplier,\n"
                + "			item\n"
                + "		where\n"
                + "			s_suppkey = i_suppkey\n"
                + "			and p_partkey = i_partkey\n"
                + "			and p_name like '%green%'\n"
                + "	) as profit\n"
                + "group by\n"
                + "	nation,\n"
                + "	order_year\n"
                + "order by\n"
                + "	nation,\n"
                + "	order_year desc;";
        executeQuery(sql);
    }

    public void query10() {
        String sql;
        sql = "select\n"
                + "	c_custkey,\n"
                + "	c_name,\n"
                + "	sum(i_extendedprice * (1 - i_discount)) as revenue,\n"
                + "	c_acctbal,\n"
                + "	c_nation_name,\n"
                + "	c_address,\n"
                + "	c_phone,\n"
                + "	c_comment\n"
                + "from\n"
                + "	customer,\n"
                + "	item\n"
                + "where\n"
                + "	c_custkey = i_custkey\n"
                + "	and i_orderdate >= date '1993-10-01'\n"
                + "	and i_orderdate < date '1993-10-01' + interval '3' month\n"
                + "	and i_returnflag = 'R'\n"
                + "group by\n"
                + "	c_custkey,\n"
                + "	c_name,\n"
                + "	c_acctbal,\n"
                + "	c_phone,\n"
                + "	c_nation_name,\n"
                + "	c_address,\n"
                + "	c_comment\n"
                + "order by\n"
                + "	revenue desc\n"
                + "limit 20;";
        executeQuery(sql);
    }

    public void query11() {
        String sql;
        sql = "select\n"
                + "	i_partkey,\n"
                + "	sum(i_supplycost * i_availqty) as value\n"
                + "from\n"
                + "	item,\n"
                + "	supplier\n"
                + "where\n"
                + "	i_suppkey = s_suppkey\n"
                + "	and s_nation_name = 'GERMANY'\n"
                + "group by\n"
                + "	i_partkey having\n"
                + "		sum(i_supplycost * i_availqty) > (\n"
                + "			select\n"
                + "				sum(i_supplycost * i_availqty) * y\n"
                + "			from\n"
                + "				item,\n"
                + "				supplier\n"
                + "			where\n"
                + "				i_suppkey = s_suppkey\n"
                + "				and s_nation_name = 'GERMANY'\n"
                + "		)\n"
                + "order by\n"
                + "	value desc;";
        executeQuery(sql);
    }

    public void query12() {
        String sql;
        sql = "select\n"
                + "	i_shipmode,\n"
                + "	sum(case\n"
                + "		when i_orderpriority = '1-URGENT'\n"
                + "			or i_orderpriority = '2-HIGH'\n"
                + "			then 1\n"
                + "		else 0\n"
                + "	end) as high_line_count,\n"
                + "	sum(case\n"
                + "		when i_orderpriority <> '1-URGENT'\n"
                + "			and i_orderpriority <> '2-HIGH'\n"
                + "			then 1\n"
                + "		else 0\n"
                + "	end) as low_line_count\n"
                + "from\n"
                + "	item\n"
                + "where\n"
                + "	and i_shipmode in ('MAIL', 'SHIP')\n"
                + "	and i_commitdate < i_receiptdate\n"
                + "	and i_shipdate < i_commitdate\n"
                + "	and i_receiptdate >= date '1994-01-01'\n"
                + "	and i_receiptdate < date '1994-01-01' + interval '1' year\n"
                + "group by\n"
                + "	i_shipmode\n"
                + "order by\n"
                + "	i_shipmode;";
        executeQuery(sql);
    }

    public void query13() {
        String sql;
        sql = "select\n"
                + "        c_count,\n"
                + "        count(*) as custdist\n"
                + "from\n"
                + "        (\n"
                + "                select\n"
                + "                        c_custkey,\n"
                + "                        count(i_itemkey)\n"
                + "                from\n"
                + "                        customer left outer join item on\n"
                + "                                c_custkey = i_custkey\n"
                + "                                and i_ordercomment not like '%special%requests%'\n"
                + "                group by\n"
                + "                        c_custkey\n"
                + "        ) as c_orders (c_custkey, c_count)\n"
                + "group by\n"
                + "        c_count\n"
                + "order by\n"
                + "        custdist desc,\n"
                + "        c_count desc;";
        executeQuery(sql);
    }

    public void query14() {
        String sql;
        sql = "select\n"
                + "	100.00 * sum(case\n"
                + "		when p_type like 'PROMO%'\n"
                + "			then i_extendedprice + 0.0 * (1 - i_discount) + 0.0\n"
                + "		else 0\n"
                + "	end) / sum(i_extendedprice + 0.0 * (1 - i_discount) + 0.0 ) as promo_revenue\n"
                + "from\n"
                + "	item,\n"
                + "	part\n"
                + "where\n"
                + "	i_partkey = p_partkey\n"
                + "	and i_shipdate >= date '1995-09-01'\n"
                + "	and i_shipdate < date '1995-09-01' + interval '1' month;";
        executeQuery(sql);
    }

    public void query15() {
        String sql;
        Statement st = null;
        sql = "create view revenue0 (supplier_no, total_revenue) as\n"
                + "	select\n"
                + "		i_suppkey,\n"
                + "		sum(i_extendedprice * (1 - i_discount))\n"
                + "	from\n"
                + "		item\n"
                + "	where\n"
                + "		i_shipdate >= date '1996-01-01'\n"
                + "		and i_shipdate < date '1996-01-01' + interval '3' month\n"
                + "	group by\n"
                + "		i_suppkey;";
        try {
            st = con.createStatement();
            st.executeUpdate(sql);
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueries.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                st.close();
            } catch (SQLException ex) {
                Logger.getLogger(TPCHQueries.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        sql = "select\n"
                + "	s_suppkey,\n"
                + "	s_name,\n"
                + "	s_address,\n"
                + "	s_phone,\n"
                + "	total_revenue\n"
                + "from\n"
                + "	supplier,\n"
                + "	revenue0\n"
                + "where\n"
                + "	s_suppkey = supplier_no\n"
                + "	and total_revenue = (\n"
                + "		select\n"
                + "			max(total_revenue)\n"
                + "		from\n"
                + "			revenue0\n"
                + "	)\n"
                + "order by\n"
                + "	s_suppkey;";
        executeQuery(sql);

        sql = "drop view revenue0;";

        try {
            st = con.createStatement();
            st.executeUpdate(sql);
            st.close();
        } catch (SQLException ex) {
            Logger.getLogger(TPCHQueries.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void query16() {
        String sql;
        sql = "select\n"
                + "        p_brand,\n"
                + "        p_type,\n"
                + "        p_size,\n"
                + "        count(distinct i_suppkey) as supplier_cnt\n"
                + "from\n"
                + "		item,\n"
                + "        part\n"
                + "where\n"
                + "        p_partkey = i_partkey\n"
                + "        and p_brand <> 'Brand#45'\n"
                + "        and p_type not like 'MEDIUM POLISHED%'\n"
                + "        and p_size in (49, 14, 23, 45, 19, 3, 36, 9)\n"
                + "        and i_suppkey not in (\n"
                + "                select\n"
                + "                        s_suppkey\n"
                + "                from\n"
                + "                        supplier\n"
                + "                where\n"
                + "                        s_comment like '%Customer%Complaints%'\n"
                + "        )\n"
                + "group by\n"
                + "        p_brand,\n"
                + "        p_type,\n"
                + "        p_size\n"
                + "order by\n"
                + "        supplier_cnt desc,\n"
                + "        p_brand,\n"
                + "        p_type,\n"
                + "        p_size;";
        executeQuery(sql);
    }

    public void query17() {
        String sql;
        sql = "select\n"
                + "        sum(i_extendedprice) / 7.0 as avg_yearly\n"
                + "from\n"
                + "        item,\n"
                + "        part\n"
                + "where\n"
                + "        p_partkey = i_partkey\n"
                + "        and p_brand = 'Brand#23'\n"
                + "        and p_container = 'MED BOX'\n"
                + "        and i_quantity < (\n"
                + "                select\n"
                + "                        0.2 * avg(i_quantity)\n"
                + "                from\n"
                + "                        item\n"
                + "                where\n"
                + "                        i_partkey = p_partkey\n"
                + "        );";
        executeQuery(sql);
    }

    public void query18() {
        String sql;
        sql = "select\n"
                + "        c_name,\n"
                + "        c_custkey,\n"
                + "        i_itemkey,\n"
                + "        i_orderdate,\n"
                + "        i_ordertotalprice,\n"
                + "        sum(i_quantity)\n"
                + "from\n"
                + "        customer,\n"
                + "        item\n"
                + "where\n"
                + "        i_itemkey in (\n"
                + "                select\n"
                + "                        i_itemkey\n"
                + "                from\n"
                + "                        item\n"
                + "                group by\n"
                + "                        i_itemkey having\n"
                + "                                sum(i_quantity) > 300\n"
                + "        )\n"
                + "        and c_custkey = i_custkey\n"
                + "group by\n"
                + "        c_name,\n"
                + "        c_custkey,\n"
                + "        i_itemkey,\n"
                + "        i_orderdate,\n"
                + "        i_ordertotalprice\n"
                + "order by\n"
                + "        i_ordertotalprice desc,\n"
                + "        i_orderdate"
                + "limit 100;";
        executeQuery(sql);
    }

    public void query19() {
        String sql;
        sql = "select\n"
                + "        sum(i_extendedprice* (1 - i_discount)) as revenue\n"
                + "from\n"
                + "        item,\n"
                + "        part\n"
                + "where\n"
                + "        (\n"
                + "                p_partkey = i_partkey\n"
                + "                and p_brand = 'Brand#12'\n"
                + "                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n"
                + "                and i_quantity >= 1 and i_quantity <= 1 + 10\n"
                + "                and p_size between 1 and 5\n"
                + "                and i_shipmode in ('AIR', 'AIR REG')\n"
                + "                and i_shipinstruct = 'DELIVER IN PERSON'\n"
                + "        )\n"
                + "        or\n"
                + "        (\n"
                + "                p_partkey = i_partkey\n"
                + "                and p_brand = 'Brand#23'\n"
                + "                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n"
                + "                and i_quantity >= 10 and i_quantity <= 10 + 10\n"
                + "                and p_size between 1 and 10\n"
                + "                and i_shipmode in ('AIR', 'AIR REG')\n"
                + "                and i_shipinstruct = 'DELIVER IN PERSON'\n"
                + "        )\n"
                + "        or\n"
                + "        (\n"
                + "                p_partkey = i_partkey\n"
                + "                and p_brand = 'Brand#34'\n"
                + "                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n"
                + "                and i_quantity >= 20 and i_quantity <= 20 + 10\n"
                + "                and p_size between 1 and 15\n"
                + "                and i_shipmode in ('AIR', 'AIR REG')\n"
                + "                and i_shipinstruct = 'DELIVER IN PERSON'\n"
                + "        );";
        executeQuery(sql);
    }

    public void query20() {
        String sql;
        sql = "select\n"
                + "        s_name,\n"
                + "        s_address\n"
                + "from\n"
                + "        supplier\n"
                + "where\n"
                + "        s_suppkey in (\n"
                + "                select\n"
                + "                        i_suppkey\n"
                + "                from\n"
                + "                        item\n"
                + "                where\n"
                + "                        i_partkey in (\n"
                + "                                select\n"
                + "                                        p_partkey\n"
                + "                                from\n"
                + "                                        part\n"
                + "                                where\n"
                + "                                        p_name like 'forest%'\n"
                + "                        )\n"
                + "                        and i_availqty > (\n"
                + "                                select\n"
                + "                                        0.5 * sum(i_quantity)\n"
                + "                                from\n"
                + "                                        item\n"
                + "                                where\n"
                + "                                        and i_shipdate >= date '1994-01-01'\n"
                + "                                        and i_shipdate < date '1994-01-01' + interval '1' year\n"
                + "                        )\n"
                + "        )\n"
                + "        and s_nation_name = 'CANADA'\n"
                + "order by\n"
                + "        s_name;";
        executeQuery(sql);
    }

    public void query21() {
        String sql;
        sql = "select\n"
                + "        s_name,\n"
                + "        count(*) as numwait\n"
                + "from\n"
                + "        supplier,\n"
                + "        item i1\n"
                + "where\n"
                + "        s_suppkey = i1.i_suppkey\n"
                + "        and i1.i_orderstatus = 'F'\n"
                + "        and i1.i_receiptdate > i1.i_commitdate\n"
                + "        and exists (\n"
                + "                select\n"
                + "                        *\n"
                + "                from\n"
                + "                        item i2\n"
                + "                where\n"
                + "                        i2.i_itemkey = i2.i_itemkey\n"
                + "                        and i2.i_suppkey <> i1.i_suppkey\n"
                + "        )\n"
                + "        and not exists (\n"
                + "                select\n"
                + "                        *\n"
                + "                from\n"
                + "                        item i3\n"
                + "                where\n"
                + "                        i3.i_itemkey = i1.i_itemkey\n"
                + "                        and i3.i_suppkey <> i1.i_suppkey\n"
                + "                        and i3.i_receiptdate > i3.i_commitdate\n"
                + "        )\n"
                + "        and s_nation_name = 'SAUDI ARABIA'\n"
                + "group by\n"
                + "        s_name\n"
                + "order by\n"
                + "        numwait desc,\n"
                + "        s_name"
                + "limit 100;";
        executeQuery(sql);
    }

    public void query22() {
        String sql;
        sql = "select\n"
                + "        cntrycode,\n"
                + "        count(*) as numcust,\n"
                + "        sum(c_acctbal) as totacctbal\n"
                + "from\n"
                + "        (\n"
                + "                select\n"
                + "                        substring(c_phone from 1 for 2) as cntrycode,\n"
                + "                        c_acctbal\n"
                + "                from\n"
                + "                        customer\n"
                + "                where\n"
                + "                        substring(c_phone from 1 for 2) in\n"
                + "                                ('13', '31', '23', '29', '30', '18', '17')\n"
                + "                        and c_acctbal > (\n"
                + "                                select\n"
                + "                                        avg(c_acctbal)\n"
                + "                                from\n"
                + "                                        customer\n"
                + "                                where\n"
                + "                                        c_acctbal > 0.00\n"
                + "                                        and substring(c_phone from 1 for 2) in\n"
                + "                                                ('13', '31', '23', '29', '30', '18', '17')\n"
                + "                        )\n"
                + "                        and not exists (\n"
                + "                                select\n"
                + "                                        *\n"
                + "                                from\n"
                + "                                        item\n"
                + "                                where\n"
                + "                                        i_custkey = c_custkey\n"
                + "                        )\n"
                + "        ) as custsale\n"
                + "group by\n"
                + "        cntrycode\n"
                + "order by\n"
                + "        cntrycode;";
        executeQuery(sql);
    }

}
