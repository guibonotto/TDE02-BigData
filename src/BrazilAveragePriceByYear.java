import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BrazilAveragePriceByYear {

    public static class BrazilMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text year = new Text();
        private DoubleWritable price = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;

            String[] columns = value.toString().split(";");
            if (columns.length > 5) {
                String country = columns[0].trim();
                if ("Brazil".equalsIgnoreCase(country)) {
                    try {
                        String transactionYear = columns[1].trim();
                        double transactionPrice = Double.parseDouble(columns[5].trim());

                        year.set(transactionYear);
                        price.set(transactionPrice);
                        context.write(year, price);
                    } catch (NumberFormatException e) {
                        // Ignora linhas com preço inválido
                    }
                }
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                result.set(sum / count);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Brazil Average Price by Year");
        job.setJarByClass(BrazilAveragePriceByYear.class);
        job.setMapperClass(BrazilMapper.class);
        job.setReducerClass(AverageReducer.class); // Não é seguro usar combiner aqui
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}