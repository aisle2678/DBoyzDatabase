import Algorithm.Algorithm;
import StorageEntity.DataSource;
import StorageEntity.OutputBuff;
import StorageEntity.StorageManager;
import StorageEntity.VirtualBuff;
import api.Filter;
import api.PreProjector;

/**
 * Created by Yi on 4/22/2016.
 */
public class TPCH_query10 {
    public static void main(String[] args){
        StorageManager sm = new StorageManager();
        Algorithm algorithm = new Algorithm(Algorithm.HASH_JOIN, sm, sm.dbInfo);
        VirtualBuff res;

//        DataSource ds1 = new DataSource("orders", sm)
//                .attachFilter(new Filter("orders.o_orderdate", ">=", "1994-12-01"))
//                .attachFilter(new Filter("orders.o_orderdate", "<", "1995-03-01"))
//                .attachPreProjector(new PreProjector("orders.o_custkey"))
//                .attachPreProjector(new PreProjector("orders.o_orderkey"))
//                .attachPreProjector(new PreProjector("orders.o_orderdate"));
//
//        DataSource ds2 = new DataSource("lineitem", sm)
//                .attachFilter(new Filter("lineitem.l_returnflag", "=", "R"))
//                .attachPreProjector(new PreProjector("lineitem.l_orderkey"))
//                .attachPreProjector(new PreProjector("lineitem.l_returnflag"))
//                .attachPreProjector(new PreProjector("l_extendedprice"))
//                .attachPreProjector(new PreProjector("l_discount"));
//
//        res = ds1.join(ds2, "orders.o_orderkey", "lineitem.l_orderkey", algorithm);
//
//
//        DataSource ds3 = new DataSource("customer", sm);
//        ds3.attachPreProjector(new PreProjector("customer.c_custkey")).attachPreProjector(new PreProjector("customer.c_name"))
//                .attachPreProjector(new PreProjector("customer.c_acctbal")).attachPreProjector(new PreProjector("customer.c_address"))
//                .attachPreProjector(new PreProjector("customer.c_phone")).attachPreProjector(new PreProjector("customer.c_comment"))
//                .attachPreProjector(new PreProjector("customer.c_nationkey"));
//        res = res.join(ds3, "orders.o_custkey", "customer.c_custkey", algorithm);
//
//        DataSource ds4 = new DataSource("nation", sm);
//        ds4.attachPreProjector(new PreProjector("nation.n_name")).attachPreProjector(new PreProjector("nation.n_nationkey"));
//        res = res.join(ds4, "customer.c_nationkey", "nation.n_nationkey", algorithm);

//        PostProjector pp = new PostProjector();
//        pp.add("customer.c_custkey").add("customer.c_name")
//                .add("customer.c_acctbal").add("nation.n_name").add("customer.c_address").add("customer.c_phone").add("customer.c_comment");

//        OutputBuff ob = new OutputBuff(res, sm, 10);
//        ob.attachPostProjector(pp);
//        ob.groupBy();
//        ob.orderBy("customer.c_custkey");
//        ob.limit(5);
//        ob.genOutput();
    }
}
