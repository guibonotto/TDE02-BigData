import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CountryYearWritable implements WritableComparable<CountryYearWritable> {

    private final Text country;
    private final Text year;

    public CountryYearWritable(String country, String year) {
        this.country = new Text(country);
        this.year = new Text(year);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        country.write(out);
        year.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country.readFields(in);
        year.readFields(in);
    }

    @Override
    public int compareTo(CountryYearWritable o) {
        int cmp = this.country.compareTo(o.country);
        if (cmp != 0) {
            return cmp;
        }
        return this.year.compareTo(o.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(country, year);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CountryYearWritable that = (CountryYearWritable) obj;
        return Objects.equals(country, that.country) && Objects.equals(year, that.year);
    }

    @Override
    public String toString() {
        return country + "\t" + year;
    }
}