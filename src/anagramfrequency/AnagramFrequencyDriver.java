package anagramfrequency;

import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class AnagramFrequencyDriver {
	
	private static Scanner scanner = new Scanner(System.in);

	public static void main(String[] args) {
		
		System.out.println ("Provide the name of your Input Folder: ");
		String inputFolderName = scanner.nextLine();
		
		System.out.println ("Specify the Output Reading Number: ");
		String outputNumber = scanner.nextLine();
		
		System.out.println ("Provide the number of running Datanodes for Output folder creation: ");
		String dataNodesNumber = scanner.nextLine();

		String outputFolderName = "AGOP" + outputNumber + inputFolderName + "_" + dataNodesNumber + "Nodes";
		System.out.println ("Here is your Output Folder Name: " + outputFolderName);
		
		System.out.println ("Your MapReduce task for " + inputFolderName + " has started................." );
		
		JobClient client = new JobClient();
		JobConf conf = new JobConf(
				anagramfrequency.AnagramFrequencyDriver.class);

		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(conf, new Path("/user/idcuser/" + inputFolderName + "/*/*/"));
	    FileOutputFormat.setOutputPath(conf, new Path("/user/idcuser/" + outputFolderName + "/"));

		conf.setMapperClass(anagramfrequency.AnagramMapper.class);
		conf.setCombinerClass(anagramfrequency.AnagramReducer.class);
		conf.setReducerClass(anagramfrequency.AnagramReducer.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
