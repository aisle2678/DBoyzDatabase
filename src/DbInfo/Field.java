package DbInfo;

/**
 * Created by Yi on 4/20/2016.
 */
public class Field {
    public String name, type;
    public int pos;
    public boolean needIndex;

    public Field(String name, String type, int pos, boolean needIndex){
        this.name = name;
        this.type = type;
        this.pos = pos;
        this.needIndex = needIndex;
    }
}
