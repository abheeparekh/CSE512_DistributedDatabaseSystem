
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin {

  	public static class JoinMapper extends Mapper<Object, Text, DoubleWritable, Text>{

    	// private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();
    	private DoubleWritable joinColumn = new DoubleWritable();

    	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    		StringTokenizer itr = new StringTokenizer(value.toString(), ",");
    		int iteration = 0;
    		while (itr.hasMoreTokens()) {
        		word.set(itr.nextToken());
        		if (iteration == 1){
            		// get join cloumn value (column 1), which is the key of map reduce
            		joinColumn.set(Double.parseDouble(word.toString().trim()));
        		}
        		iteration += 1;
			}
			// the key is the join column
			// the value is entire row
    		context.write(joinColumn, value);
    		//   System.out.println("key is " + joinColumn + "value is " + value);
    	}
  	}

  	public static class JoinReducer
    	extends Reducer<DoubleWritable,Text,NullWritable,Text> {
    	private Text result = new Text();
    
		public void reduce(DoubleWritable key, Iterable<Text> values,Context context) 
		throws IOException, InterruptedException {
    		ArrayList<String> array1 = new ArrayList<String>();
    		ArrayList<String> array2 = new ArrayList<String>();
    		String tableName1 = null;
    		String tableName2 = null;
    		for (Text val : values) {
        		StringTokenizer itr = new StringTokenizer(val.toString(), ",");
        		String tableName = itr.nextToken();
				// the table name 1 and table name 2 will be assigned based on the input data
				// the values corresponding to that table name will be added to the arraylist
        		if (tableName1 == null ){
            		tableName1 = tableName;
            		array1.add(val.toString());
        		}else if (tableName1.compareTo(tableName)== 0){
            		array1.add(val.toString());
        		}else if(tableName2 == null ){
            		tableName2 = tableName;
            		array2.add(val.toString());
        		}else if (tableName2.compareTo(tableName) ==0){
            		array2.add(val.toString());
        		}
      		}
    		//   System.out.println("table 1 " + tableName1);
    		//   System.out.println("table 2 " + tableName2);
    		//   for(String i: array1){
    		//       System.out.println("array 1" + i);
    		//   }
    		//   for(String i: array2){
    		//     System.out.println("array 2" + i);
    		// }

			// perform equijoin operation
    		for (String i:array1){
        		for(String j:array2){
            		// System.out.println("Reducer out "+ i + " " + j);
            		result.set(i + ", " +  j);
            		context.write(null, result);
        		}
    		}
    	}
  	}

	public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "equijoin");
    job.setJarByClass(equijoin.class);
    job.setMapperClass(JoinMapper.class);
    // job.setCombinerClass(JoinReducer.class);
    job.setReducerClass(JoinReducer.class);
    job.setOutputKeyClass(DoubleWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}
}