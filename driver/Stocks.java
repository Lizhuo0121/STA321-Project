package driver;

import mapper.OrderInputMapper;
import mapper.TradeInputMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import reducer.FinalReducer;
import sort.Sort;

import java.io.IOException;

public class Stocks {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();//create a Configuration object
        Job job = Job.getInstance(conf, "Stocks");//create a job
        job.setJarByClass(Stocks.class);//initialize the job

        String inputPath = args[0];//the input path of original data as an argument
        String outputPath = args[1];//the output path of result data as an argument

        //multiple input
        //OrderInputMapper
        MultipleInputs.addInputPath(job, new Path(inputPath+"/am_hq_order_spot.txt"), TextInputFormat.class, OrderInputMapper.class);//order data dealed with OrderInputMapper
        MultipleInputs.addInputPath(job, new Path(inputPath+"/pm_hq_order_spot.txt"), TextInputFormat.class, OrderInputMapper.class);//pm order data input
        //TradeInputMapper
        MultipleInputs.addInputPath(job, new Path(inputPath+"/am_hq_trade_spot.txt"), TextInputFormat.class, TradeInputMapper.class);//trade data dealed with TradeInputMapper
        MultipleInputs.addInputPath(job, new Path(inputPath+"/pm_hq_trade_spot.txt"), TextInputFormat.class, TradeInputMapper.class);//pm trade data input


        // 这里重复使用了MultipleInputMapper2类因为实现逻辑完全一样
        job.setReducerClass(FinalReducer.class);//set the reducer as FinalReducer
        job.setOutputKeyClass(Text.class);//set the output key class
        job.setOutputValueClass(Text.class);//set the output value class

        TextOutputFormat.setOutputPath(job, new Path(outputPath));//set the output path

        if (job.waitForCompletion(true)){//if the job complete successfully, then we sort the output file.
            Sort.Sort(outputPath+"/part-r-00000",outputPath+"/you.csv");//use Sort to sort the output data,and write the new data into outputPath+\you.csv
            System.exit(0);//exit
        }else System.exit(1);//if job not finished successfully, exit with status 1
    }
}
