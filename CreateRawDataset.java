package SensorDataCheck;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;

public class CreateRawDataset {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		FileSystem hdfs =FileSystem.get(conf);
				
		Calendar cal=Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
    	int dayt=cal.get(Calendar.DAY_OF_MONTH);
    	int month=cal.get(Calendar.MONTH)+1;
    	int year=cal.get(Calendar.YEAR);
    	String D = Integer.toString(dayt);
    	String M = Integer.toString(month);
    	String Y = Integer.toString(year);

    	    	
    	String inrix = M + "-" + D + "-" + Y + ".csv";
    	

		Path hdfsFilePath3=new Path("Tingting/SensorData/inrix_raw.txt");
		Path hdfsFilePath4=new Path("INRIX_XD/IOWAv2/" + Y + "/"+ M + "/" + inrix);
			
		//hdfs.concat(hdfsFilePath1,sourcePath);

		FileUtil.copy(hdfs,hdfsFilePath4,hdfs,hdfsFilePath3,false,true,conf);
		
    	if (dayt<10){D = "0" + D;}
    	if (month<10){M = "0" + M;}
    	String wave = M + D + Y + ".txt";
    	
		Path hdfsFilePath1=new Path("Tingting/SensorData/wave_raw.txt");
		Path hdfsFilePath2=new Path("Shuo/wavetronix/" + Y + M + "/" + wave);
		
		FileUtil.copy(hdfs,hdfsFilePath2,hdfs,hdfsFilePath1,false,true,conf);
	}
	
}


