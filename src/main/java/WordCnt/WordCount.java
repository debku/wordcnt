package WordCnt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.TextOutputFormat;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration config = new Configuration();

        String[] files=new GenericOptionsParser(config,args).getRemainingArgs();

        Path input=new Path(files[0]);

        Path output=new Path(files[1]);

        Job job=new Job(config,"wordcount");

        job.setJarByClass(WordCount.class);

        job.setMapperClass(Map.class);

        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);

        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true)?0:1);

    }

    //code for mapper phase

    public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{

        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {

            //setting up variables line and tokenizer.Variable line stores a row from the file as a string.Variable tokenizer is used to break a string into tokens.

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

           // iterating over the tokens

            while(tokenizer.hasMoreTokens()){
                value.set(tokenizer.nextToken());
                context.write(value,new IntWritable(1));
            }

        }
    }



    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{

        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;

            //iterating over the values corresponding to a particular key

            for(IntWritable x : values){
                sum = sum + x.get();
                context.write(key,new IntWritable(sum));
            }
        }
    }
}
