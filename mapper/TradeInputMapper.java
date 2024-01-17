package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TradeInputMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString(); //Transform the value into a string

        if (input.contains("\u0009000001\u0009")) { // Determine whether the SecurityID is 000001
            String[] inputs = input.split("\u0009"); // Split the string into inputs of tokens by space
            int time = Integer.parseInt(inputs[15].substring(8)); // Extract the tradetime token as time

            if ((93000000 <= time && time <= 113000000) | (130000000 <= time && time <= 145700000)) {// Filter trades within our time frame
                Text BidApplSeqNum = new Text(inputs[10]); // Set the key for the first output as BidApplSeqNum

                Text OfferApplSeqNum = new Text(inputs[11]); // Set the key for the second output as OfferApplSeqNum

                Text v2 = new Text(); // Define v2 for both outputs as their value are identical
                v2.set(inputs[12] + " " + inputs[13] + " " + inputs[14] + " " + inputs[15]); // Fill the value with a string consists of Price, TradeQty, ExecType and tradetime

                context.write(BidApplSeqNum, v2); // output one key-value pair of BidApplSeqNum
                context.write(OfferApplSeqNum, v2); // output another key-value pair of OfferApplSeqNum
            }
        }
    }
}
