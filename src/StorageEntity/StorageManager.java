package StorageEntity;

import DbInfo.DbInfo;

import java.util.ArrayList;

/**
 * Created by Yi on 4/20/2016.
 */
public class StorageManager {
    public DbInfo dbInfo;

    /**
     * This structure stores actual values of disk files.
     * The key is the type of the value.
     */
    private ArrayList<ArrayList<String>> buffPool;

    private int SUBLIST_SIZE = 10240;

    public StorageManager(){
        this.dbInfo = new DbInfo();
        this.buffPool = new ArrayList<>();
        this.buffPool.add(new ArrayList<>(SUBLIST_SIZE));
    }

    public VirtualBuff allocate(){
        return new VirtualBuff(this, this.dbInfo);
    }

    public int[] store(String value){
        ArrayList<String> subList = buffPool.get(buffPool.size() - 1);
        if (subList.size() < SUBLIST_SIZE){
            subList.add(value);
        }else{
            subList = new ArrayList<>(SUBLIST_SIZE);
            subList.add(value);
            buffPool.add(subList);
        }
        int[] addr = new int[2];
        addr[0] = buffPool.size() - 1;
        addr[1] = subList.size() - 1;

        return addr;
    }

    public String retrieve(int[] addr){
        return buffPool.get(addr[0]).get(addr[1]);
    }
}
