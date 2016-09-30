import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class K_ass {

	static final boolean DEBUG = false;

	public static void debug(Object o, String s) {
		if (DEBUG) {
			System.out.println(s + ":" + o.toString());
		}
	}

	public static List<ArrayList<Double>> getOldCenters(String inputPath) {
		List<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>();
		Configuration conf = new Configuration();
		try {
			//System.out.println("getOladCenters:0");
			Path inPath = new Path(inputPath);
			FileSystem hdfs = inPath.getFileSystem(conf);
			FSDataInputStream fsIn = hdfs.open(inPath);
			LineReader lineIn = new LineReader(fsIn, conf);
			Text line = new Text();
			while (lineIn.readLine(line) > 0) {

				String record = line.toString();
				String[] fields = record.split(",");
				List<Double> tmpList = new ArrayList<Double>();
				for (int i = 0; i < fields.length; i++)
					tmpList.add(Double.parseDouble(fields[i]));
				result.add((ArrayList<Double>) tmpList);
				
			}
			//System.out.println("getOladCenters:1");
			lineIn.close();
			fsIn.close();
			
		} catch (IOException e) {

			e.printStackTrace();
		}

		return result;
	}

	public static void deleteLastResult(String path) {
		Configuration conf = new Configuration();
		try {
			//FileSystem hdfs = FileSystem.get(conf);
			Path inPath = new Path(path);
			FileSystem hdfs = inPath.getFileSystem(conf);
			System.out.println(inPath.toString());
			hdfs.delete(inPath,true);
		} catch (IOException e) {

		}
	}

	public static void deleteLastResult_f(String path) {
		Configuration conf = new Configuration();
		try {
			//FileSystem hdfs = FileSystem.get(conf);
			Path inPath = new Path(path);
			FileSystem hdfs = inPath.getFileSystem(conf);
			System.out.println(inPath.toString());
			hdfs.delete(inPath,false);
		} catch (IOException e) {

		}
	}
	
	public static void copyOriginalCenters(String src, String dst) {
		Configuration conf = new Configuration();
		try {
			Path outPath=new Path(dst);
			FileSystem hdfs = outPath.getFileSystem(conf);
			hdfs.copyFromLocalFile(false,new Path(src), new Path(dst));
			System.out.println();
		} catch (IOException e) {

		}
	}
	
	public static void initCenters(String src, String dst,int K) {
		Configuration conf = new Configuration();
		try {
			Path outPath=new Path(dst);
			Path inPath=new Path(src+"/test_data");//+"/k-means-in");
			Text line=new Text();
			FileSystem hdfsin = inPath.getFileSystem(conf);
			FSDataInputStream is = hdfsin.open(inPath);
			LineReader lineIn = new LineReader(is, conf);
			int numLine=0;
			while (lineIn.readLine(line) > 0) {
				//String temp=line.toString();
				numLine++;
				//System.out.println("num:"+numLine);
			}
			int Seek=(int)(Math.random()*numLine);
			int num=(int)(numLine/K);
			List<Integer> centerPos=new ArrayList<Integer>();
			for(int i=0;i<K;i++){
				centerPos.add((K+num*i)%numLine);
				
			}
			//System.out.println("pos"+centerPos);
			int pos=0;
			lineIn.close();
			is.close();
			is=hdfsin.open(inPath);
			lineIn=new LineReader(is, conf);
			FileSystem hdfsout = outPath.getFileSystem(conf);
			FSDataOutputStream os = hdfsout.create(outPath);
			while(lineIn.readLine(line) > 0){
				pos++;
				for(int j=0;j<K;j++){
					if(pos==centerPos.get(j)){
						String temp=line.toString()+"\n";
						//System.out.println(" "+temp);
						os.write(temp.getBytes(),0,temp.length());
						break;
					}
				}
			}
			os.close();
			lineIn.close();
			is.close();
			
			//os.write(time.toString().getBytes(),0, time.toString().length());
		} catch (IOException e) {

		}
	}

	public static boolean isFinished(String oldPath, String newPath,
			String KPath, String dtBegIdxPath, double threshold)
			throws IOException {
		//System.out.println("isFinished :0");
		int dataBeginIndex = Integer.parseInt(dtBegIdxPath);
		int K = Integer.parseInt(KPath);
		//System.out.println("isFinished :0.1");
		List<ArrayList<Double>> oldCenters = K_ass.getOldCenters(oldPath);
		List<ArrayList<Double>> newCenters = new ArrayList<ArrayList<Double>>();
		int[] recordPosition=new int[K];
		//System.out.println("isFinished :0.2");
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		//System.out.println("isFinished :1");
		for (int t = 0; t < K; t++) {
			Path inPath = new Path(newPath + t);
			hdfs=inPath.getFileSystem(conf);
			if (!hdfs.exists(inPath)){
				
				//System.out.println("isFinished :1.0"+inPath);
				break;}
			FSDataInputStream fsIn = hdfs.open(inPath);
			LineReader lineIn = new LineReader(fsIn, conf);
			Text line = new Text();
			//System.out.println("isFinished :1_1");
			while (lineIn.readLine(line) > 0) {
				String tmp = line.toString();
				K_ass.debug("tmp", tmp);
				//System.out.println("isFinished :1_2 "+tmp);
				if(tmp.length()<5)//处理在集群上出现的key与value不在一行的情况
				{
					lineIn.readLine(line);
					System.out.println(line.toString());
					tmp = line.toString();
					String []fields = tmp.split(",");
					List<Double> tmpList = new ArrayList<Double>();
					for (int i = 0; i < fields.length; i++)
						tmpList.add(Double.parseDouble(fields[i]));
					newCenters.add((ArrayList<Double>) tmpList);
					continue;
				}
					
				String[] tmpLine = tmp.split("	");
				K_ass.debug(tmpLine[1].toString(), tmpLine.toString());
				String record = tmpLine[1];
				String[] fields = record.split(",");
				List<Double> tmpList = new ArrayList<Double>();
				for (int i = 0; i < fields.length; i++)
					tmpList.add(Double.parseDouble(fields[i]));
				newCenters.add((ArrayList<Double>) tmpList);
				recordPosition[Integer.parseInt(tmpLine[0])-1]=t;
			}
			lineIn.close();
			fsIn.close();
			
		}

		// System.out.println("oldCenter size:"+oldCenters.get(0).size()+"\nnewCenters size:"+newCenters.size());
		//System.out.println("isFinished :2");
		double distance = 0;
		for (int i = 0; i < K; i++) {
			System.out.println("isFinished :2.0 "+dataBeginIndex+" "+oldCenters.get(i).size());
			for (int j = dataBeginIndex; j < oldCenters.get(0).size(); j++) {
				//System.out.println("isFinished :2.0 round"+i+"*"+j );
				double t1 = Math.abs(oldCenters.get(i).get(j));
				//System.out.println(t1);
				double t2 = Math.abs(newCenters.get(recordPosition[i]).get(j));
				//System.out.println(t2);
				distance += Math.pow((t1 - t2)/(t1 + t2), 2);
				//distance += Math.pow(Math.abs(t1 - t2), 2);
			}
		}
		//System.out.println(distance/oldCenters.get(0).size());
		//System.out.println("isFinished :2.1");
		if (distance/oldCenters.get(0).size() <= threshold) {
			return true;
		}
		//System.out.println("isFinished :3");
		K_ass.deleteLastResult(oldPath);
		FSDataOutputStream os = hdfs.create(new Path(oldPath));

		for (int i = 0; i < newCenters.size(); i++) {
			String text = "";
			for (int j = 0; j < newCenters.get(recordPosition[i]).size(); j++) {
				if (j == 0)
					text += newCenters.get(recordPosition[i]).get(j);
				else
					text += "," + newCenters.get(recordPosition[i]).get(j);
			}
			text += "\n";
			os.write(text.getBytes(), 0, text.length());
		}
		os.close();
		//System.out.println("isFinished :4");
		// ///////////////////////////
		return false;
	}
}
