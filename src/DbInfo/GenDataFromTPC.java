package DbInfo;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by Yi on 4/12/2016.
 *
 */
public class GenDataFromTPC {
    public static void main(String args[]){
        DbInfo dbInfo = new DbInfo();

        String line;
        for (Map.Entry<String, Table> entry: dbInfo.tables.entrySet()){
            Table table = entry.getValue();
            String file = table.name;

            try {
                FileReader fileReader = new FileReader(dbInfo.TPC_DIR + dbInfo.PATH_DELIMITER + file + ".tbl");
                BufferedReader bufferedReader = new BufferedReader(fileReader);

                while((line = bufferedReader.readLine()) != null) {
                    generateDB(line, table, dbInfo);
                }
                flushBuffs(table, dbInfo);
                bufferedReader.close();

                System.out.println("RowBaseDB written: " + rowBasedWrittenSize);
                System.out.println("ColBaseDB written: " + colBasedWrittenSize);
            }
            catch(FileNotFoundException ex) {
                System.out.println("Unable to open file '" + file + "'");
            }
            catch(IOException ex) {
                System.out.println("Error reading file '" + file + "'");
            }
        }
    }

    static int readSize;
    static int rowBasedWrittenSize = 0, colBasedWrittenSize = 0;
    static StringBuilder rowBasedBuff = new StringBuilder();
    static List<StringBuilder> colBasedBuff = new LinkedList<>();
    public static void generateDB(String line, Table table, DbInfo dbInfo) throws FileNotFoundException, UnsupportedEncodingException {
        int listLackCount = table.fields.size() - colBasedBuff.size();
        if (listLackCount > 0){
            for (int i = 0; i < listLackCount; i++){
                colBasedBuff.add(new StringBuilder());
            }
        }

        readSize += line.length();
        if (readSize >= dbInfo.FILE_SIZE) {
            rowBasedWrittenSize += writeRowBasedSplit(rowBasedBuff, table, dbInfo);
            colBasedWrittenSize += writeColBasedSplit(colBasedBuff, table, dbInfo);
            readSize = line.length();
            rowBasedBuff = new StringBuilder();
            rowBasedBuff.append(line).append('\n');
            colBasedBuffAccumulate(line, true);
        } else {
            rowBasedBuff.append(line).append('\n');
            colBasedBuffAccumulate(line, false);
        }
    }

    public static long writeRowBasedSplit(StringBuilder data, Table table, DbInfo dbInfo) throws FileNotFoundException, UnsupportedEncodingException {
        long start_time, end_time;

        start_time = System.nanoTime();
        String dirName = dbInfo.ROW_DB_DIR + dbInfo.PATH_DELIMITER + table.name;
        new File(dirName).mkdirs();
        //todo write binary files
        PrintWriter writer = new PrintWriter(dirName + dbInfo.PATH_DELIMITER + table.rowBasedSplitCount, "UTF-8");
        writer.println(data.toString());
        writer.close();
        end_time = System.nanoTime();
        System.out.println("create " + dirName + "-->" + table.rowBasedSplitCount + " time:" + (end_time-start_time)/1e9 + " sec");
        table.rowBasedSplitCount++;
        return data.length();
    }

    public static long writeColBasedSplit(List<StringBuilder> data, Table table, DbInfo dbInfo) throws FileNotFoundException, UnsupportedEncodingException {
        long start_time, end_time;
        start_time = System.nanoTime();
        String dirName = dbInfo.COL_DB_DIR + dbInfo.PATH_DELIMITER + table.name;
        new File(dirName).mkdirs();
        //todo write binary files
        PrintWriter writer = new PrintWriter(dirName + dbInfo.PATH_DELIMITER + table.colBasedSplitCount, "UTF-8");

        long writtenSize = 0;

        for (StringBuilder b: data){
            writtenSize += b.length();
            writer.println(b.toString());
        }
        writer.close();
        end_time = System.nanoTime();
        System.out.println("create " + dirName + "-->" + table.colBasedSplitCount + " time:" + (end_time-start_time)/1e9 + " sec");
        table.colBasedSplitCount++;

        return writtenSize;
    }

    public static void colBasedBuffAccumulate(String line, boolean reset){
        if (reset){
            for (int i = 0; i < colBasedBuff.size(); i++){
                colBasedBuff.set(i, new StringBuilder());
            }
        }

        int index = 0;
        String splitFlag = "\\|";
        String[] splits = line.split(splitFlag);
        for (String str: splits){
            StringBuilder builder = colBasedBuff.get(index);
            builder.append(str).append('|');
            colBasedBuff.set(index, builder);
            index++;
        }
    }

    public static void flushBuffs(Table table, DbInfo dbInfo) throws FileNotFoundException, UnsupportedEncodingException {
        writeRowBasedSplit(rowBasedBuff, table, dbInfo);
        writeColBasedSplit(colBasedBuff, table, dbInfo);

        readSize = 0;
        rowBasedBuff = new StringBuilder();
        colBasedBuff = new LinkedList<>();
    }
}
