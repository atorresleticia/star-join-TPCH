/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tpch;

import java.sql.Connection;
import queries.RefreshFunctionTime;
import queries.TPCHQueriesTime;

/**
 *
 * @author LTorres
 */
public class Power {
    
    private final int sf;
    private final Connection con;
    private final TPCHQueriesTime tpchQT;
    private final RefreshFunctionTime rfT;
    private final String sgbd;

    public Power(Connection con, int sf, String sgbd) {
        this.sf = sf;
        this.con = con;
        this.tpchQT = new TPCHQueriesTime();
        this.rfT = new RefreshFunctionTime();
        this.sgbd = sgbd;
        setConnection();
    }

    public final void setConnection() {
        this.tpchQT.getTpchq().setConnection(this.con);
        this.rfT.getRF1().setConnection(this.con);
        this.rfT.getRF2().setConnection(this.con);
    }

    public void run() {
//        String path = String.format("D:\\TPCH\\DATA\\DENORMALIZED\\RF\\%dGB\\delete.u1", this.sf);
        String path = String.format("/home/leticia/tcc_files/DATA/DENORMALIZED/RF/%dGB/delete.u1", this.sf);
        rfT.getRF2().getIndexes(path);
        
        rfT.runRF1();
        tpchQT.run();
        rfT.runRF2();

        saveTimes();
    }
    
    public void saveTimes(){
        String rfFileName = String.format("/home/leticia/tcc_files/saidas/%d/%s_STAR_POWER_RF_%dGB", this.sf, this.sgbd, this.sf);
        String queriesFileName = String.format("/home/leticia/tcc_files/saidas/%d/%s_STAR_POWER_QUERIES_%dGB", this.sf, this.sgbd, this.sf);
        
        rfT.saveRFTime(rfFileName);
        tpchQT.saveQueriesTime(queriesFileName);
    }

    public double[] getQueryExecutionTime() {
        return this.tpchQT.getTimes();
    }

    public double[] getRefreshExecutionTime() {
        return this.rfT.getTimes();
    }

}
