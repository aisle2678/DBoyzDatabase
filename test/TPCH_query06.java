import Algorithm.Algorithm;
import StorageEntity.DataSource;
import StorageEntity.StorageManager;
import StorageEntity.VirtualBuff;
import api.Filter;
import api.PreProjector;

import java.util.Iterator;

/**
 * Created by Yi on 5/10/2016.
 */
public class TPCH_query06 {
    public static void main(String[] args){
        long startTime, endTime;

        startTime = System.nanoTime();
        run(DataSource.ROW_BASED);
        endTime = System.nanoTime();

        System.out.println("Row Based Database Time cost: " + (endTime - startTime)/1e9);

        startTime = System.nanoTime();
        run(DataSource.COLUMN_BASED);
        endTime = System.nanoTime();

        System.out.println("Column Based Database Time cost: " + (endTime - startTime)/1e9);
    }

    private static void run(int dbType){
        StorageManager sm = new StorageManager();
        Algorithm algorithm = new Algorithm(Algorithm.HASH_JOIN, sm, sm.dbInfo);
        VirtualBuff res;
        DataSource ds;

        ds = new DataSource("lineitem", sm, dbType)
                .attachFilter(new Filter("lineitem.l_shipdate", ">=", "1995-01-01"))
                .attachFilter(new Filter("lineitem.l_shipdate", "<", "1996-01-01"))
                .attachFilter(new Filter("lineitem.l_discount", ">=", "0.01"))
                .attachFilter(new Filter("lineitem.l_discount", "<=", "0.03"))
                .attachFilter(new Filter("lineitem.l_quantity", "<", "25"))
                .attachPreProjector(new PreProjector("lineitem.l_shipdate"))
                .attachPreProjector(new PreProjector("lineitem.l_discount"))
                .attachPreProjector(new PreProjector("lineitem.l_extendedprice"))
                .attachPreProjector(new PreProjector("lineitem.l_quantity"));
        res = ds.read();

        Iterator itr1 = res.getValueListItr("lineitem.l_extendedprice");
        Iterator itr2 = res.getValueListItr("lineitem.l_discount");
        double revenue = 0;
        while (itr1.hasNext()){
            String str1 = sm.retrieve((int[]) itr1.next());
            String str2 = sm.retrieve((int[]) itr2.next());
            double a = Double.parseDouble(str1);
            double b = Double.parseDouble(str2);
            revenue += a * b;
        }
        System.out.println("revenue: " + revenue);
    }
}
