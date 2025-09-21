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

public class BrazilMinMaxPrice2016 {

    public static class Brazil2016Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private static final Text keyout = new Text("Brazil2016Stats");
        private DoubleWritable price = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;

            String[] columns = value.toString().split(";");
            if (columns.length > 5) {
                String country = columns[0].trim();
                String year = columns[1].trim();
                if ("Brazil".equalsIgnoreCase(country) && "2016".equals(year)) {
                    try {
                        price.set(Double.parseDouble(columns[5].trim()));
                        context.write(keyout, price);
                    } catch (NumberFormatException e) {
                        // Ignora
                    }
                }
            }
        }
    }

    public static class MinMaxReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double minPrice = Double.MAX_VALUE;
            double maxPrice = Double.MIN_VALUE;

            for (DoubleWritable val : values) {
                double price = val.get();
                if (price < minPrice) {
                    minPrice = price;
                }
                if (price > maxPrice) {
                    maxPrice = price;
                }
            }
            String result = String.format("Min: %.2f, Max: %.2f", minPrice, maxPrice);
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Brazil Min-Max Price 2016");
        job.setJarByClass(BrazilMinMaxPrice2016.class);
        job.setMapperClass(Brazil2016Mapper.class);
        job.setReducerClass(MinMaxReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}