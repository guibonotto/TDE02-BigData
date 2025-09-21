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

public class MinMaxAmountByCountryYear {

    public static class AmountMapper extends Mapper<LongWritable, Text, CountryYearWritable, DoubleWritable> {
        private DoubleWritable amount = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) return;

            String[] columns = value.toString().split(";");
            // `Amount` é a coluna de índice 8
            if (columns.length > 8) {
                try {
                    String country = columns[0].trim();
                    String year = columns[1].trim();
                    double transactionAmount = Double.parseDouble(columns[8].trim());

                    if (!country.isEmpty() && !year.isEmpty()) {
                        CountryYearWritable compositeKey = new CountryYearWritable(country, year);
                        amount.set(transactionAmount);
                        context.write(compositeKey, amount);
                    }
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                    // Ignora linhas malformadas
                }
            }
        }
    }

    public static class MinMaxReducer extends Reducer<CountryYearWritable, DoubleWritable, CountryYearWritable, Text> {
        @Override
        protected void reduce(CountryYearWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double minAmount = Double.MAX_VALUE;
            double maxAmount = Double.MIN_VALUE;

            for (DoubleWritable val : values) {
                double amount = val.get();
                minAmount = Math.min(minAmount, amount);
                maxAmount = Math.max(maxAmount, amount);
            }
            String result = String.format("MinAmount: %.2f, MaxAmount: %.2f", minAmount, maxAmount);
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Min-Max Amount by Country and Year");
        job.setJarByClass(MinMaxAmountByCountryYear.class);

        job.setMapperClass(AmountMapper.class);
        job.setReducerClass(MinMaxReducer.class);

        job.setMapOutputKeyClass(CountryYearWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(CountryYearWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}