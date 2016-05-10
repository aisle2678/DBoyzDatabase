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
public class TPCH_query17 {
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
        DataSource ds1, ds2;
        ds1 = new DataSource("part", sm, dbType)
                .attachPreProjector(new PreProjector("part.p_partkey"));

        ds2 = new DataSource("lineitem", sm, dbType)
                .attachPreProjector(new PreProjector("lineitem.l_quantity"))
                .attachPreProjector(new PreProjector("lineitem.l_partkey"));

        res = ds1.join(ds2, "part.p_partkey", "lineitem.l_partkey", algorithm);

        Iterator itr = res.getValueListItr("lineitem.l_quantity");
        double avgQuantity = 0;
        int count = 0;
        while (itr.hasNext()){
            String str = sm.retrieve((int[]) itr.next());
            double a = Double.parseDouble(str);
            avgQuantity += a;
            count++;
        }
        avgQuantity = avgQuantity / count * 0.2;


        ds1 = new DataSource("part", sm, dbType)
                .attachFilter(new Filter("part.p_brand", "=", "Brand#23"))
                .attachFilter(new Filter("part.p_container", "=", "MED BOX"))
                .attachPreProjector(new PreProjector("part.p_brand"))
                .attachPreProjector(new PreProjector("part.p_container"))
                .attachPreProjector(new PreProjector("part.p_partkey"));

        ds2 = new DataSource("lineitem", sm, dbType)
                .attachFilter(new Filter("lineitem.l_quantity", "<", Double.toString(avgQuantity)))
                .attachPreProjector(new PreProjector("lineitem.l_quantity"))
                .attachPreProjector(new PreProjector("lineitem.l_partkey"))
                .attachPreProjector(new PreProjector("lineitem.l_extendedprice"));

        res = ds1.join(ds2, "part.p_partkey", "lineitem.l_partkey", algorithm);

        itr = res.getValueListItr("lineitem.l_extendedprice");

        double avgExtendedPrice = 0;
        while (itr.hasNext()){
            String str = sm.retrieve((int[]) itr.next());
            double a = Double.parseDouble(str);
            avgExtendedPrice += a;
        }
        System.out.println("avg Extended Price : " + avgExtendedPrice / 7.0);         //3864732.125799999
    }
}
