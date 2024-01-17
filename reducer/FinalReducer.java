package reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class FinalReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> marketPrices = new HashSet<>(); // The set of traded prices
        List<String[]> tradeRecords = new ArrayList<>(); // The list of traded records without cancel
        String[] orderRecord = new String[5]; // The order that involved in traded records
        String[] cancelRecord = new String[4]; // The cancel record of an order

        for (Text value : values) { // Iterate through the list of values
            String input = value.toString(); // Transform one value into a string
            String[] inputs = input.split(" "); // Split the string into inputs of tokens by space
            if (inputs.length == 5) { // Check the inputs' length to determine if the value is from order mapper with length 5
                orderRecord = inputs; // Fill the orderRecord with the value from order mapper
            } else if (inputs[2].equals("4")) { // Check ExecType of the value to see if it is a cancel trade
                cancelRecord = inputs; // Fill the cancelRecord with the value from trade mapper that is a cancel trade
            } else {
                tradeRecords.add(inputs); // Throw the rest value from trade mapper that represent actual trade into tradeRecords
                marketPrices.add(inputs[0]); // Add the trading prices into the set of marketPrices
            }
        }

        Text value = new Text(); // Define the output

        if (orderRecord[0] != null) { // Exclude the situation where there is no corresponding order for a delayed trade
            int time1 = Integer.parseInt(orderRecord[2].substring(8)); // Extract TransactTime from orderRecord
            if (tradeRecords.size() != 0 && ((93000000 <= time1 && time1 <= 113000000) | (130000000 <= time1 && time1 <= 145700000))) { // Filter the orders with time frame and check if it is traded
                switch (orderRecord[4]) { // The order is traded, check for its OrderType
                    case "1": // The order is a market order
                        value.set(timeTransform(orderRecord[2]) + "," + "0.0" + "," + orderRecord[1] + "," + orderRecord[3] + "," + "1" + "," + key.toString() + "," + marketPrices.size() + "," + "2"); // set the corresponding value
                        context.write(NullWritable.get(), value); // output the value
                        break;
                    case "2": // The order is a limited order
                        value.set(timeTransform(orderRecord[2]) + "," + orderRecord[0].substring(0,orderRecord[0].indexOf(".")+3) + "," + orderRecord[1] + "," + orderRecord[3] + "," + "2" + "," + key.toString() + "," + "," + "2"); // set the corresponding value
                        context.write(NullWritable.get(), value); // output the value
                        break;
                    case "U": // The order is a self-best order
                        value.set(timeTransform(orderRecord[2]) + "," + "0.0" + "," + orderRecord[1] + "," + orderRecord[3] + "," + "2" + "," + key.toString() + "," + "," + "2"); // set the corresponding value
                        context.write(NullWritable.get(), value); // output the value
                        break;
                }
            } else if ((93000000 <= time1 && time1 <= 113000000) | (130000000 <= time1 && time1 <= 145700000)) { // The order is not traded, output the original order
                value.set(timeTransform(orderRecord[2]) + "," + orderRecord[0].substring(0,orderRecord[0].indexOf(".")+3) + "," + orderRecord[1] + "," + orderRecord[3] + "," + "2" + "," + key.toString() + "," + "," + "2"); // set the corresponding value
                context.write(NullWritable.get(), value); // output the value
            }
            if (cancelRecord[0] != null) { // Part of / all of the order is canceled, output a cancel record
                value.set(timeTransform(cancelRecord[3]) + "," + "0.0" + "," + cancelRecord[1] + "," + orderRecord[3] + "," + orderRecord[4] + "," + key.toString() + "," + "," + "1"); // set the corresponding value
                context.write(NullWritable.get(), value); // output the value
            }
        }
    }

    static String timeTransform(String time){ // Transform the time string into requested format
        StringBuilder sb = new StringBuilder(time); // Define a new string
        sb.insert(4,"-"); // Year-Month Separation
        sb.insert(7,"-"); // Month-Data Separation
        sb.insert(10," "); // Date Hour Separation
        sb.insert(13,":"); // Hour:Minute Separation
        sb.insert(16,":"); // Minute:Second Separation
        sb.insert(19,"."); // Second.Millisecond Separation
        sb.append("000"); // Millisecond Addition
        return sb.toString(); // return the formatted time
    }
}




