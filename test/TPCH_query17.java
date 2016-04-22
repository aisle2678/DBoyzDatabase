import Algorithm.Algorithm;
import StorageEntity.DataSource;
import StorageEntity.StorageManager;
import StorageEntity.VirtualBuff;

/**
 * Created by Yi on 4/22/2016.
 */
public class TPCH_query17 {
    StorageManager sm = new StorageManager();
    Algorithm algorithm = new Algorithm(Algorithm.HASH_JOIN, sm, sm.dbInfo);
    VirtualBuff res;
    DataSource ds1, ds2;
    long startTime, endTime;
}
