package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderInputMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString(); // Transform the input value into a string

        if (input.contains("\u0009000001\u0009")) { // Determine whether the SecurityID is 000001
            String[] inputs = input.split("\u0009"); // Split the input string into inputs of tokens by space
            int time = Integer.parseInt(inputs[12].substring(8)); // Extract the TransactTime token as time
            if ((time <= 145700000)) { // Filter orders latter than our frame time
                Text AppSeqNum = new Text(inputs[7]); // Set the ApplSeqNum as the key in output of key-value pair

                Text valueOutput = new Text(); // Define the value in output
                valueOutput.set(inputs[10] + " " + inputs[11] + " " + inputs[12] + " " + inputs[13] + " " + inputs[14]); // Fill the value with a string consists of Price, OrderQty, TransactTime, Side and OrderType

                context.write(AppSeqNum, valueOutput); // output the key-value pair
            }
        }
    }
}