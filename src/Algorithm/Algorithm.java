package Algorithm;

import DbInfo.DbInfo;
import StorageEntity.StorageManager;
import StorageEntity.VirtualBuff;

import java.util.*;

/**
 * Created by Yi on 4/20/2016.
 */
public class Algorithm {
    public static int NESTED_LOOP_JOIN = 1;
    public static int MERGE_JOIN = 2;
    public static int HASH_JOIN = 3;

    private int type;
    private StorageManager sm;
    private DbInfo dbInfo;

    public Algorithm(int type, StorageManager sm, DbInfo dbInfo){
        this.type = type;
        this.sm = sm;
        this.dbInfo = dbInfo;
    }

    public VirtualBuff run(VirtualBuff b1, VirtualBuff b2, String attr1, String attr2){
        if (type == HASH_JOIN){
            return hashJoin(b1, b2, attr1, attr2);
        }
        return null;
    }

    private VirtualBuff hashJoin(VirtualBuff b1, VirtualBuff b2, String attr1, String attr2) {
        VirtualBuff vb = sm.allocate();
        Set<String> b1_valueSet = b1.getColumnValueSet(attr1);

        for (String b1Value : b1_valueSet) {
            if (b2.hasValue(attr2, b1Value)) {
                LinkedList<Integer> b1ViewNums = b1.getLineNums(attr1, b1Value);
                LinkedList<Integer> b2ViewNums = b2.getLineNums(attr2, b1Value);
                for (int b1LineNum: b1ViewNums){
                    for (int b2LineNum: b2ViewNums){
                        Iterator<int[]> b1AddrList = b1.getViewLine(b1LineNum).iterator();
                        Iterator<int[]> b2AddrList = b2.getViewLine(b2LineNum).iterator();
                        Iterator<String> b1ColumnNames = b1.getViewAttrs().iterator();
                        Iterator<String> b2ColumnNames = b2.getViewAttrs().iterator();

                        while (b1AddrList.hasNext()){
                            vb.addSingleValue(b1ColumnNames.next(), b1AddrList.next());
                        }
                        while (b2AddrList.hasNext()){
                            vb.addSingleValue(b2ColumnNames.next(), b2AddrList.next());
                        }
                    }
                }
            }
        }
        return vb;
    }
}
