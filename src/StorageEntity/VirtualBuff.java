package StorageEntity;

import Algorithm.Algorithm;
import DbInfo.DbInfo;

import java.io.IOException;
import java.util.*;

/**
 * Created by Yi on 4/20/2016.
 */
public class VirtualBuff {
    private LinkedHashMap<String, ArrayList<int[]>> view;
    private HashMap<String, HashMap<String, LinkedList<Integer>>> index;
    private StorageManager sm;
    private DbInfo dbInfo;

    public VirtualBuff(StorageManager sm, DbInfo dbInfo){
        this.view = new LinkedHashMap<>();
        this.index = new HashMap<>();
        this.sm = sm;
        this.dbInfo = dbInfo;
    }

    public void addSingleValue(String columnName, int[] addr){
        int viewLineNum = this.addSingleViewValue(columnName, addr);
        if (dbInfo.needIndex(columnName)){
            addSingleIndexValue(columnName, addr, viewLineNum);
        }
    }

    private int addSingleViewValue(String columnName, int[] addr){
        ArrayList<int[]> valueList;
        if (view.containsKey(columnName)){
            valueList = view.get(columnName);
        }else{
            valueList = new ArrayList<>();
            view.put(columnName, valueList);
        }
        valueList.add(addr);
        return valueList.size() - 1;
    }

    private void addSingleIndexValue(String columnName, int[] addr, int viewLineNum){
        String value = sm.retrieve(addr);
        getIndexList(columnName, value).add(viewLineNum);
    }

    public LinkedList<int[]> getViewLine(int lineNum){
        LinkedList<int[]> res = new LinkedList<>();
        for (Map.Entry<String, ArrayList<int[]>> entry: view.entrySet()){
            res.add(entry.getValue().get(lineNum));
        }
        return res;
    }

    public Set<String> getViewAttrs(){
        return view.keySet();
    }

    public Iterator getValueListItr(String columnName){
        return view.get(columnName).iterator();
    }

    public VirtualBuff join(DataSource b, String selfAttr, String otherAttr, Algorithm algorithm){
        VirtualBuff otherBuff, res = sm.allocate();
        try {
            while ((otherBuff = b.getNextBlock()) != null) {
                res = res.merge(algorithm.run(this, otherBuff, selfAttr, otherAttr));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    public VirtualBuff merge(VirtualBuff b){
        if (this.isEmpty()){
            return b;
        }
        if (b.isEmpty()){
            return this;
        }

        assert (this.view.size() == b.view.size());
        for (Map.Entry<String, ArrayList<int[]>> entry: b.view.entrySet()){
            for (int[] addr: entry.getValue()){
                this.addSingleValue(entry.getKey(), addr);
            }
        }

        return this;
    }

    public boolean hasValue(String columnName, String value){
        return !(!index.containsKey(columnName) || (!index.get(columnName).containsKey(value)));
    }

    public boolean isEmpty(){
        return (view.size() == 0);
    }

    public Set<String> getColumnValueSet(String columnName){
        assert (index.containsKey(columnName));
        return index.get(columnName).keySet();
    }

    public LinkedList<Integer> getLineNums(String columnName, String value){
        return index.get(columnName).get(value);
    }

    private LinkedList<Integer> getIndexList(String column, String value){
        HashMap<String, LinkedList<Integer>> columnMap;
        LinkedList<Integer> lineNumList;
        if (index.containsKey(column)){
            columnMap = index.get(column);
        }else{
            columnMap = new HashMap<>();
            index.put(column, columnMap);
        }

        if (columnMap.containsKey(value)){
            lineNumList = columnMap.get(value);
        }else{
            lineNumList = new LinkedList<>();
            columnMap.put(value, lineNumList);
        }
        return lineNumList;
    }
}
