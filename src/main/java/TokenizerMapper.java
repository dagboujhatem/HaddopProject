
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        // split each line
        StringTokenizer itr = new StringTokenizer(value.toString(),",");
        // skipping the first token
        if(itr.hasMoreTokens()) {
            itr.nextToken();
        }
        String governantFiliereKey = "";
        while (itr.hasMoreTokens()) {

            governantFiliereKey += itr.nextToken();

            if(itr.hasMoreTokens()) {
                governantFiliereKey += "-";
            }
        }
        System.out.println("Add the key :"+governantFiliereKey + " and the value is : 1");
            word.set(governantFiliereKey);
            context.write(word, one);

    }
}
