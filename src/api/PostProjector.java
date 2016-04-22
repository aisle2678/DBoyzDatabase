package api;

import java.util.LinkedList;

/**
 * Created by Yi on 4/20/2016.
 */
public class PostProjector {
    public LinkedList<String> projectors;

    public PostProjector(){
        this.projectors = new LinkedList<>();
    }

    public PostProjector add(String str){
        this.projectors.add(str);
        return this;
    }
}
