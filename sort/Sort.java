package sort;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;

class StringComparator implements Comparator<String> {
    @Override
    public int compare(String s1, String s2) {//override the compareTo method
        String[] part1 = s1.split(",");//split the String by ",", then the array is timeStamp, price, size, buy_sell_flag, order_type, order_id, market order type, cancel type.
        String[] part2 = s2.split(",");//split the String by ","
        if(!part1[0].equals(part2[0])){//if the timeStamp is not equal with each other,then return the result between timestamps
            return part1[0].compareTo(part2[0]);//return the compare result between timestamps
        }else if (part1[7].compareTo(part2[7]) != 0){//secondly,if the cancelType is not equal with each other,then return the result between cancelType
            return -part1[7].compareTo(part2[7]);//return the compare result between timestamps
        }else {
            return part1[5].compareTo(part2[5]);//finally, return the compare result between order id
        }
    }
}

public class Sort {
    public static void Sort(String inputFilePath, String outputFilePath) throws IOException {
        Configuration conf = new Configuration();//create a Configuration object, to connect the HDFS
        FileSystem fs = FileSystem.get(conf);//create a FileSystem object
        FSDataInputStream inputStream = fs.open(new Path(inputFilePath));//to read the file in HDFS as Stream input

        // read the file
        ArrayList<String> lines = new ArrayList<>();//create an Arraylist to store lines
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {//speed up the read process by bufferedreader
            String line;//create String
            while ((line = reader.readLine()) != null) {//if the file has next
                lines.add(line);//store in to the arraylist
            }
        }

        // sort the arraylist
        Collections.sort(lines, new StringComparator());//sort the arraylist by collections.sort using the StringComparator above.

        // Create an HDFS file output stream. true indicates that data is appended if the file exists
        FSDataOutputStream outputStream = fs.create(new Path(outputFilePath), true);//Create an HDFS file output stream

        // Writes the sorted contents back to the file
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {//speed up the read process by bufferedwriter
            writer.write("TIMESTAMP,PRICE,SIZE,BUY_SELL_FLAG,ORDER_TYPE,ORDER_ID,MARKET_ORDER_TYPE,CANCEL_TYPE");//write the column name into the first
            writer.newLine();//newline
            for (String line : lines) {//write every line in the arraylist
                writer.write(line);//write into the file
                writer.newLine();//newline
            }
        }

        // The file system connection is closed
        fs.close();
    }
}
