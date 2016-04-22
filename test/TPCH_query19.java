import Algorithm.Algorithm;
import StorageEntity.DataSource;
import StorageEntity.StorageManager;
import StorageEntity.VirtualBuff;
import api.Filter;
import api.PreProjector;

import java.util.Iterator;

/**
 * Created by Yi on 4/22/2016.
 */
public class TPCH_query19 {
    public static void main(String[] args){
        long startTime, endTime;

//        startTime = System.nanoTime();
//        run(DataSource.ROW_BASED);
//        endTime = System.nanoTime();
//
//        System.out.println("Time cost: " + (endTime - startTime)/1e9);

        startTime = System.nanoTime();
        run(DataSource.COLUMN_BASED);
        endTime = System.nanoTime();

        System.out.println("Time cost: " + (endTime - startTime)/1e9);
    }

    private static void run(int dbType){
        StorageManager sm = new StorageManager();
        Algorithm algorithm = new Algorithm(Algorithm.HASH_JOIN, sm, sm.dbInfo);
        VirtualBuff res;
        DataSource ds1, ds2;
        ds1 = new DataSource("part", sm, dbType)
                .attachFilter(new Filter("part.p_brand", "=", "Brand#31"))
                .attachFilter(new Filter("part.p_container", "in", "SM CASE,SM BOX,SM PACK,SM PKG"))
                .attachFilter(new Filter("part.p_size", "<=", "5"))
                .attachFilter(new Filter("part.p_size", ">=", "1"))
                .attachPreProjector(new PreProjector("part.p_brand"))
                .attachPreProjector(new PreProjector("part.p_container"))
                .attachPreProjector(new PreProjector("part.p_size"))
                .attachPreProjector(new PreProjector("part.p_partkey"));

        ds2 = new DataSource("lineitem", sm, dbType)
                .attachFilter(new Filter("lineitem.l_quantity", ">=", "8"))
                .attachFilter(new Filter("lineitem.l_quantity", "<=", "18"))
                .attachFilter(new Filter("lineitem.l_shipmode", "in", "AIR,AIR REG"))
                .attachFilter(new Filter("lineitem.l_shipinstruct", "=", "DELIVER IN PERSON"))
                .attachPreProjector(new PreProjector("lineitem.l_quantity"))
                .attachPreProjector(new PreProjector("lineitem.l_shipmode"))
                .attachPreProjector(new PreProjector("lineitem.l_shipinstruct"))
                .attachPreProjector(new PreProjector("lineitem.l_partkey"))
                .attachPreProjector(new PreProjector("lineitem.l_extendedprice"))
                .attachPreProjector(new PreProjector("lineitem.l_discount"));

        res = ds1.join(ds2, "part.p_partkey", "lineitem.l_partkey", algorithm);


        ds1 = new DataSource("part", sm, dbType)
                .attachFilter(new Filter("part.p_brand", "=", "Brand#55"))
                .attachFilter(new Filter("part.p_container", "in", "MED BAG,MED BOX,MED PKG,MED PACK"))
                .attachFilter(new Filter("part.p_size", "<=", "10"))
                .attachFilter(new Filter("part.p_size", ">=", "1"))
                .attachPreProjector(new PreProjector("part.p_brand"))
                .attachPreProjector(new PreProjector("part.p_container"))
                .attachPreProjector(new PreProjector("part.p_size"))
                .attachPreProjector(new PreProjector("part.p_partkey"));

        ds2 = new DataSource("lineitem", sm, dbType)
                .attachFilter(new Filter("lineitem.l_quantity", ">=", "13"))
                .attachFilter(new Filter("lineitem.l_quantity", "<=", "23"))
                .attachFilter(new Filter("lineitem.l_shipmode", "in", "AIR,AIR REG"))
                .attachFilter(new Filter("lineitem.l_shipinstruct", "=", "DELIVER IN PERSON"))
                .attachPreProjector(new PreProjector("lineitem.l_quantity"))
                .attachPreProjector(new PreProjector("lineitem.l_shipmode"))
                .attachPreProjector(new PreProjector("lineitem.l_shipinstruct"))
                .attachPreProjector(new PreProjector("lineitem.l_partkey"))
                .attachPreProjector(new PreProjector("lineitem.l_extendedprice"))
                .attachPreProjector(new PreProjector("lineitem.l_discount"));

        res = res.merge(ds1.join(ds2, "part.p_partkey", "lineitem.l_partkey", algorithm));


        ds1 = new DataSource("part", sm, dbType)
                .attachFilter(new Filter("part.p_brand", "=", "Brand#22"))
                .attachFilter(new Filter("part.p_container", "in", "LG CASE,LG BOX,LG PACK,LG PKG"))
                .attachFilter(new Filter("part.p_size", "<=", "15"))
                .attachFilter(new Filter("part.p_size", ">=", "1"))
                .attachPreProjector(new PreProjector("part.p_brand"))
                .attachPreProjector(new PreProjector("part.p_container"))
                .attachPreProjector(new PreProjector("part.p_size"))
                .attachPreProjector(new PreProjector("part.p_partkey"));

        ds2 = new DataSource("lineitem", sm, dbType)
                .attachFilter(new Filter("lineitem.l_quantity", ">=", "25"))
                .attachFilter(new Filter("lineitem.l_quantity", "<=", "35"))
                .attachFilter(new Filter("lineitem.l_shipmode", "in", "AIR,AIR REG"))
                .attachFilter(new Filter("lineitem.l_shipinstruct", "=", "DELIVER IN PERSON"))
                .attachPreProjector(new PreProjector("lineitem.l_quantity"))
                .attachPreProjector(new PreProjector("lineitem.l_shipmode"))
                .attachPreProjector(new PreProjector("lineitem.l_shipinstruct"))
                .attachPreProjector(new PreProjector("lineitem.l_partkey"))
                .attachPreProjector(new PreProjector("lineitem.l_extendedprice"))
                .attachPreProjector(new PreProjector("lineitem.l_discount"));

        res = res.merge(ds1.join(ds2, "part.p_partkey", "lineitem.l_partkey", algorithm));

        Iterator itr1 = res.getValueListItr("lineitem.l_extendedprice");
        Iterator itr2 = res.getValueListItr("lineitem.l_discount");

        double revenue = 0;
        while (itr1.hasNext()){
            String str1 = sm.retrieve((int[]) itr1.next());
            String str2 = sm.retrieve((int[]) itr2.next());
            double a = Double.parseDouble(str1);
            double b = Double.parseDouble(str2);
            revenue += (a * (1 - b));
        }
        System.out.println("revenue: " + revenue);         //3864732.125799999
    }
}
