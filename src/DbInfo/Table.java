package DbInfo;

import java.util.HashMap;

/**
 * Created by Yi on 4/20/2016.
 */
public class Table {
    public String name;
    public HashMap<String, Field> fields;
    public int rowBasedSplitCount, colBasedSplitCount;

    private int index;

    public Table(String name){
        this.name = name;
        fields = new HashMap<>();
        this.index = 0;
        this.rowBasedSplitCount = 0;
        this.colBasedSplitCount = 0;
    }

    public Table addFields(String fieldName, String type, boolean needIndex){
        fields.put(name+"."+fieldName, new Field(name+fieldName, type, index, needIndex));
        index++;
        return this;
    }
}
