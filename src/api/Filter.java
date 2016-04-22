package api;

import DbInfo.DbInfo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Yi on 4/20/2016.
 */
public class Filter {
    public String left, right, op;

    public Filter(String left, String op, String right){
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public boolean pass(String dbValue, DbInfo dbInfo){
        return cmp(dbValue, right, op, dbInfo.getFieldType(left));
    }

    private boolean cmp(String left, String right, String op, String type){
        if (type.equalsIgnoreCase("int")){
            int leftValue = Integer.parseInt(left);
            int rightValue = Integer.parseInt(right);
            if (((op.equals("=") || op.equals("==")) && (leftValue != rightValue))
                    || (op.equals(">") && (leftValue <= rightValue))
                    || (op.equals("<") && (leftValue >= rightValue))
                    || (op.equals(">=") && (leftValue < rightValue))
                    || (op.equals("<=") && (leftValue > rightValue))
                    || (op.equals("!=") && (leftValue == rightValue))){
                return false;
            }
        }else if(type.equalsIgnoreCase("char")){
            String leftValue = left;
            String rightValue = right;       //remove ' in the value
            if (((op.equals("=") || (op.equals("=="))) && (!leftValue.equals(rightValue)))
                    || (op.equals("!=") && leftValue.equals(rightValue))
                    || (op.equals("in") && !strInStr(left, right))){
                return false;
            }
        }else if(type.equalsIgnoreCase("date")){
            SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
            try {
                Date leftValue = fmt.parse(left);
                Date rightValue = fmt.parse(right);
                int res = leftValue.compareTo(rightValue);
                if (((op.equals("=") || op.equals("==")) && (res != 0))
                        || (op.equals(">") && (res <= 0))
                        || (op.equals("<") && (res >= 0))
                        || (op.equals(">=") && (res < 0))
                        || (op.equals("<=") && (res > 0))
                        || (op.equals("!=") && (res == 0))){
                    return false;
                }
            } catch (ParseException e) {
                e.printStackTrace();
                System.out.println("Wrong date format");
                return false;
            }
        }else if (type.equals("dec")){
            double leftValue = Double.parseDouble(left);
            double rightValue = Double.parseDouble(right);
            if (((op.equals("=") || op.equals("==")) && (leftValue != rightValue))
                    || (op.equals(">") && (leftValue <= rightValue))
                    || (op.equals("<") && (leftValue >= rightValue))
                    || (op.equals(">=") && (leftValue < rightValue))
                    || (op.equals("<=") && (leftValue > rightValue))
                    || (op.equals("!=") && (leftValue == rightValue))){
                return false;
            }
        }
        else{
            System.out.println("Fatal Error: Unknown data type, type: " + type);
            return false;
        }
        return true;
    }

    private boolean strInStr(String str1, String str2){
        String[] pool = str2.split(",");
        for (String s: pool){
            if (str1.equals(s)){
                return true;
            }
        }
        return false;
    }
}
