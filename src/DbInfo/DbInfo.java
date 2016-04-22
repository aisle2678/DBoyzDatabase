package DbInfo;

import java.util.*;

/**
 * Created by Yi on 4/20/2016.
 */
public class DbInfo {
    public String FIELD_DELIMITER = "\\|";
    public String PATH_DELIMITER = "/";
    public int FILE_SIZE = 10 * 1024 * 1024;
    public int JOIN_BUFF_SIZE = 1;      // actual size = JOIN_BUFF_SIZE * FILE_SIZE
    public String TPC_DIR = "tpc";
    public String DB_DIR = "DB";
    public String COL_DB_DIR = DB_DIR + "/" + "column_based";
    public String ROW_DB_DIR = DB_DIR + "/" + "row_based";

    public Map<String, Table> tables;
    public DbInfo(){
        this.tables = new HashMap<>();
        Table table = new Table("region");
        table.addFields("r_regionkey", "int", true).addFields("r_name", "char", false)
                .addFields("r_comment", "char", false);
        tables.put("region", table);

        table = new Table("nation");
        table.addFields("n_nationkey", "int", true).addFields("n_name", "char", false).addFields("n_regionkey", "int", true)
                .addFields("n_comment", "char", false);
        tables.put("nation", table);

        table = new Table("lineitem");
        table.addFields("l_orderkey", "int", true).addFields("l_partkey", "int", true).addFields("l_suppkey", "int", true)
                .addFields("l_linenumber", "int", false).addFields("l_quantity", "dec", false).addFields("l_extendedprice", "dec", false)
                .addFields("l_discount", "dec", false).addFields("l_tax", "dec", false).addFields("l_returnflag", "char", false)
                .addFields("l_linestatus", "char", false).addFields("l_shipdate", "date", false).addFields("l_commitdate", "date", false)
                .addFields("l_receiptdate", "date", false).addFields("l_shipinstruct", "char", false).addFields("l_shipmode", "char", false)
                .addFields("l_comment", "char", false);
        tables.put("lineitem", table);

        table = new Table("orders");
        table.addFields("o_orderkey", "int", true).addFields("o_custkey", "int", true).addFields("o_orderstatus", "char", false)
                .addFields("o_totalprice", "dec", false).addFields("o_orderdate", "date", false).addFields("o_orderpriority", "char", false)
                .addFields("o_clerk", "char", false).addFields("o_shippriority", "int", false).addFields("o_comment", "int", false);
        tables.put("orders", table);

        table = new Table("customer");
        table.addFields("c_custkey", "int", true).addFields("c_name", "char", false).addFields("c_address", "char", false)
                .addFields("c_nationkey", "int", true).addFields("c_phone", "char", false).addFields("c_acctbal", "dec", false)
                .addFields("c_mktsegment", "char", false).addFields("c_comment", "char", false);
        tables.put("customer", table);

        table = new Table("part");
        table.addFields("p_partkey", "int", true).addFields("p_name", "char", false).addFields("p_mfgr", "char", false)
                .addFields("p_brand", "char", false).addFields("p_type", "char", false).addFields("p_size", "int", false)
                .addFields("p_container", "char", false).addFields("p_retailprice", "dec", false).addFields("p_comment", "char", false);
        tables.put("part", table);

        table = new Table("supplier");
        table.addFields("s_suppkey", "int", true).addFields("s_name", "char", false).addFields("s_address", "char", false)
                .addFields("s_nationkey", "int", true).addFields("s_phone", "char", false).addFields("s_acctbal", "dec", false)
                .addFields("s_comment", "char", false);
        tables.put("supplier", table);

        table = new Table("partsupp");
        table.addFields("ps_partkey", "int", true).addFields("ps_suppkey", "int", true).addFields("ps_availqty", "int", false)
                .addFields("ps_supplycost", "dec", false).addFields("ps_comment", "char", false);
        tables.put("partsupp", table);
    }

    public int getFieldPos(String fieldName){
        return this.tables.get(fieldName.split("\\.")[0]).fields.get(fieldName).pos;
    }

    public String getFieldType(String fieldName){
        return this.tables.get(fieldName.split("\\.")[0]).fields.get(fieldName).type;
    }

    public boolean needIndex(String fieldName){
        return this.tables.get(fieldName.split("\\.")[0]).fields.get(fieldName).needIndex;
    }
}
