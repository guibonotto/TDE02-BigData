import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BrazilTransactionsCount {

    public static class TransactionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);
        private Text countryKey = new Text("Brazil");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Ignora o cabeçalho do CSV
            if (key.get() == 0) {
                return;
            }

            String line = value.toString();
            String[] columns = line.split(";");

            // Verifica se a linha tem o número esperado de colunas
            if (columns.length > 0) {
                String country = columns[0];
                if ("Brazil".equalsIgnoreCase(country.trim())) {
                    context.write(countryKey, one);
                }
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Brazil Transactions Count");

        job.setJarByClass(BrazilTransactionsCount.class);
        job.setMapperClass(TransactionMapper.class);
        job.setCombinerClass(SumReducer.class); // Otimização: pode usar o mesmo Reducer como Combiner
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}