package StorageEntity;

import api.PostProjector;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by Yi on 4/20/2016.
 */
public class OutputBuff{
    private VirtualBuff buff;
    private PostProjector pp;
    private int limit;
    private StorageManager sm;
    private String orderBy;
    private int tpchQueryNum;

    public OutputBuff(VirtualBuff b, StorageManager sm, int tpchQueryNum){
        this.buff = b;
        this.sm = sm;
        this.tpchQueryNum = tpchQueryNum;
    }

    public void attachPostProjector(PostProjector pp){
        this.pp = pp;
    }

    public void groupBy(){

    }

    public void orderBy(String str){
        this.orderBy = str;
    }

    public void limit(int limit){
        this.limit = limit;
    }

    public void genOutput(){
        if (this.tpchQueryNum == 10) {
            tpch10Output();
        }
    }

    private void tpch10Output(){
        int outputCount = 0;
        LinkedList<Iterator> itrs = new LinkedList<>();

        itrs.add(buff.getValueListItr("customer.c_custkey"));
        itrs.add(buff.getValueListItr("customer.c_name"));

        for (String s: pp.projectors){
            itrs.add(buff.getValueListItr(s));
        }
        while (true){
            if (outputCount >= limit){
                return;
            }
            for (Iterator it: itrs){
                if (!it.hasNext()){
                    return;
                }
                System.out.print(sm.retrieve((int[]) it.next()) + "\t\t");
            }
            System.out.print('\n');
            outputCount++;
        }
    }
}
