package StorageEntity;

import Algorithm.Algorithm;
import DbInfo.DbInfo;
import api.Filter;
import api.PreProjector;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by Yi on 4/20/2016.
 */
public class DataSource{
    public static int ROW_BASED = 1;
    public static int COLUMN_BASED = 2;

    private String tableName, databasePath;
    private Iterator blockItr;
    private LinkedList<Filter> filters;
    private LinkedList<PreProjector> projectors;
    private DbInfo dbInfo;
    private StorageManager sm;
    private int dbType;

    public DataSource(String tableName, StorageManager sm, int dbType){
        this.filters = new LinkedList<>();
        this.projectors = new LinkedList<>();

        this.dbInfo = sm.dbInfo;
        this.tableName = tableName;
        this.sm = sm;
        this.dbType = dbType;

        LinkedList<String> fileNameList = new LinkedList<>();
        File folder;
        if (dbType == ROW_BASED){
            this.databasePath = dbInfo.ROW_DB_DIR + dbInfo.PATH_DELIMITER + tableName + dbInfo.PATH_DELIMITER;
        }else {
            this.databasePath = dbInfo.COL_DB_DIR + dbInfo.PATH_DELIMITER + tableName + dbInfo.PATH_DELIMITER;
        }
        folder = new File(databasePath);
        File[] files = folder.listFiles();
        assert files != null;
        for (File f: files){
            fileNameList.add(f.getName());
        }
        this.blockItr = fileNameList.iterator();
    }

    public DataSource attachFilter(Filter f){
        filters.add(f);
        return this;
    }

    public DataSource attachPreProjector(PreProjector pp){
        projectors.add(pp);
        return this;
    }

    public VirtualBuff read(){
        VirtualBuff vb = sm.allocate(), tmp;
        try {
            while ((tmp = this.getNextBlock()) != null){
                vb = vb.merge(tmp);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return vb;
    }

    public VirtualBuff join(DataSource b, String selfAttr, String otherAttr, Algorithm algorithm){
        VirtualBuff selfBuff = sm.allocate(), otherBuff = sm.allocate(), res = sm.allocate();

        selfBuff = this.read();
        otherBuff = b.read();
//        try {
//            while ((selfBuff = this.getNextBlock()) != null){
//                while ((otherBuff = b.getNextBlock()) != null){
//                    VirtualBuff tmp = algorithm.run(selfBuff, otherBuff, selfAttr, otherAttr);
//                    res = res.merge(tmp);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        res = algorithm.run(selfBuff, otherBuff, selfAttr, otherAttr);
        return res;
    }

    public VirtualBuff getNextBlock() throws IOException {
        if (!blockItr.hasNext()){
            return null;
        }

        FileReader fileReader;
        fileReader = new FileReader(databasePath + blockItr.next());
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        if (this.dbType == ROW_BASED){
            return getNextBlockOfRowBasedDB(bufferedReader);
        }else{
            return getNextBlockOfColumnBasedDB(bufferedReader);
        }
    }

    private VirtualBuff getNextBlockOfRowBasedDB(BufferedReader bufferedReader) throws IOException {
        VirtualBuff vb = sm.allocate();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            if (line.trim().isEmpty()){
                continue;
            }
            String[] attrs = line.split(dbInfo.FIELD_DELIMITER);
            if (attrs.length != dbInfo.tables.get(tableName).fields.size()){
                System.out.println("Bad Line in " + tableName + ": " + line);
                continue;
            }
            if (RB_filter(attrs)){
                RB_project(attrs, vb);
            }
        }

        bufferedReader.close();
        if (vb.isEmpty()){
            return null;
        }else{
            return vb;
        }
    }

    private boolean RB_filter(String[] fields){
        for (Filter f: filters) {
            if (!f.pass(fields[dbInfo.getFieldPos(f.left)], dbInfo)) {
                return false;
            }
        }
        return true;
    }

    private void RB_project(String[] attrs, VirtualBuff vb) {
        for (PreProjector p : projectors) {
            String columnName = p.value;
            String readValue = attrs[dbInfo.getFieldPos(columnName)];
            int[] addr = sm.store(readValue);
            vb.addSingleValue(columnName, addr);
        }
    }

    private VirtualBuff getNextBlockOfColumnBasedDB(BufferedReader bufferedReader) throws IOException {
        VirtualBuff vb = sm.allocate();
        String line;
        HashMap<Integer, String> projectorMap = new HashMap<>();
        HashMap<String, String[]> projectedBuff = new HashMap<>();

        for (PreProjector p: projectors){
            projectorMap.put(dbInfo.getFieldPos(p.value), p.value);
        }

        int lineNum = 0, valueLen = 0;
        while ((line = bufferedReader.readLine()) != null) {
            if (line.trim().isEmpty()){
                continue;
            }

            if (projectorMap.containsKey(lineNum)){
                String[] values = line.split(dbInfo.FIELD_DELIMITER);
                valueLen = values.length;
                projectedBuff.put(projectorMap.get(lineNum), values);
            }
            lineNum++;
        }

        LinkedList<Integer> mask = new LinkedList<>();
        for (int index = 0; index < valueLen; index++){
            boolean pass = true;
            for (Filter f: filters){
                if (!f.pass(projectedBuff.get(f.left)[index], dbInfo)){
                    pass = false;
                    break;
                }
            }
            if (pass){
                mask.add(index);
            }
        }

        for (int i: mask){
            for (Map.Entry<String, String[]> entry: projectedBuff.entrySet()){
                int[] addr = sm.store(entry.getValue()[i]);
                vb.addSingleValue(entry.getKey(), addr);
            }
        }

        bufferedReader.close();
        if (vb.isEmpty()){
            return null;
        }else{
            return vb;
        }
    }
}
