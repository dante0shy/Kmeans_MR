import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Kmeans_MR {

	// static List<ArrayList<Double>> centers ;
	// static int K;
	// static int dataBeginIndex;
	public static int count=0;
	public static class KmeansMapper extends
			Mapper<Object, Text, IntWritable, Text> {
		List<ArrayList<Double>> centers=new ArrayList<ArrayList<Double>>();
		
		 @Override
         protected void setup(Context context) throws IOException, InterruptedException {  
			 centers = K_ass.getOldCenters(context
								.getConfiguration().get("centersPath")); 
       }  
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] fields = line.split(",");

			//List<ArrayList<Double>> centers = K_ass.getOldCenters(context
			//		.getConfiguration().get("centersPath"));
			int dataBeginIndex = Integer.parseInt(context.getConfiguration()
					.get("dtBegIdxPath"));
			int K = Integer.parseInt(context.getConfiguration().get("KPath"));

			double minDistance = 99999999;
			int centerIndex = K;
			for (int i = 0; i < K; i++) {
				double currentDistance = 0;
				for (int j = dataBeginIndex; j < fields.length; j++) {
					double t1 = Math.abs(centers.get(i).get(j));
					double t2 = Math.abs(Double.parseDouble(fields[j]));
					//System.out.println(i+"*"+j+" "+t1+"\n"+i+"*"+j+" "+t2);
					currentDistance += Math.pow((t1 - t2) / (t1 + t2), 2);
					//System.out.println(i+"*"+j+" "+currentDistance);
				}
				K_ass.debug(currentDistance, "currentDistance");
				//System.out.println(i+" "+currentDistance);
				if (minDistance > currentDistance) {
					minDistance = currentDistance;
					centerIndex = i;
				}
				//System.out.println(i+" "+minDistance +" "+centerIndex);
			}
			IntWritable centerId = new IntWritable(centerIndex+1);
			Text tValue = new Text();
			tValue.set(value);
			context.write(centerId, tValue);
		}
	}

	public static class KmeansReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			List<ArrayList<Double>> helpList = new ArrayList<ArrayList<Double>>();
			String tmpResult = "";
			for (Text val : values) {
				String line = val.toString();
				String[] fields = line.split(",");
				List<Double> tmpList = new ArrayList<Double>();
				for (int i = 0; i < fields.length; i++) {
					tmpList.add(Double.parseDouble(fields[i]));
				}
				helpList.add((ArrayList<Double>) tmpList);
			}

			// System.out.println(helpList.size());
			// for(int i=0;i<helpList.size();i++)
			// System.out.println(helpList.get(i));

			for (int i = 0; i < helpList.get(0).size(); i++) {
				double sum = 0;
				for (int j = 0; j < helpList.size(); j++) {
					sum += helpList.get(j).get(i);
				}
				double t = sum / helpList.size();
				if (i == 0)
					tmpResult += t;
				else
					tmpResult += "," + t;
			}
			//Text result = new Text();
			//result.set(tmpResult);
			//int tmpKey = Integer.parseInt(key.toString());
			context.write(key,new Text( tmpResult));
		}
	}

	static void runKmeans(String[] args, boolean isReduce) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 8) {
			System.err
					.println("Usage: Kmeans <in> <out> <localOriginalCentersPath> <oldCentersPath> <newCentersPath> <dataBeginIndex> <K>");
			System.exit(2);
		}
		
		conf.setStrings("centersPath", otherArgs[3]);
		System.out.println(otherArgs[3]);
		conf.setStrings("dtBegIdxPath", otherArgs[5]);
		System.out.println(otherArgs[5]);
		conf.setStrings("KPath", otherArgs[6]);
		System.out.println(otherArgs[6]);
		if(count==0){
			
		}
		Job job =Job.getInstance(conf, "kmeans");
		job.setJarByClass(Kmeans_MR.class);
		job.setMapperClass(KmeansMapper.class);
		job.setNumReduceTasks(Integer.parseInt(args[6]));
		// 判断是否需要执行Reduce
		if (isReduce) {
			job.setReducerClass(KmeansReducer.class);
		}
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// delete last result
		//System.out.println(otherArgs[1]);
		K_ass.deleteLastResult(otherArgs[1]);
		//System.out.println(otherArgs[1]);
		// System.exit(job.waitForCompletion(true)?0:1);
		job.waitForCompletion(true);

	}

	/**
	 * 
	 * @param in
	 *            - args[0] out - args[1] 	localOriginalCentersPath - args[2]
	 *            oldCentersPath - args[3] 	newCentersPath - args[4]
	 *            dataBeginIndex - args[5] 	K - args[6]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.out.println(args[0]);
		K_ass.deleteLastResult_f(args[3]);
		System.out.println(args[3]);
		//K_ass.copyOriginalCenters(args[2], args[3]);
		
		int max_count=100;
		//runKmeans(args, true);
		//K_ass.isFinished(args[3], args[4], args[6], args[5], 0.0);
		//runKmeans(args, true);
		//K_ass.isFinished(args[3], args[4], args[6], args[5], 0.0);
		long cal_start_time=System.currentTimeMillis();
		K_ass.initCenters(args[0], args[3], Integer.parseInt(args[6]));
		while (true) {
			//System.out.println("round： "+count++);
			runKmeans(args, true);
			//System.out.println("round： "+(count-1)+" mid");
			if (K_ass.isFinished(args[3], args[4], args[6], args[5], 0.0)&&count<=max_count) {
				runKmeans(args, false);
				long cal_end_time=System.currentTimeMillis();
				String time=String.valueOf(cal_end_time-cal_start_time)+"\n";
				Configuration conf = new Configuration();
				try {
						//FileSystem hdfs = FileSystem.get(conf);
						Path inPath = new Path(args[7]);
						FileSystem hdfs = inPath.getFileSystem(conf);
						FSDataOutputStream os = hdfs.create(inPath);
						//System.out.println(inPath.toString());
						os.write(time.toString().getBytes(),0, time.toString().length());
				} catch (IOException e) {

				}
				break;
			}
			count++;
			//System.out.println("round： "+(count-1)+" finished");
		}
	}
}
