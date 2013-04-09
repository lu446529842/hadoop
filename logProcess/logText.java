package dataMining.logProcess;

import java.io.*;
import java.util.*;

public class logText {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException{
		File file = new File("/home/yuzhe/桌面/HardDisk/hadoop项目/access_log_20120423.20120423");
		
		BufferedReader reader = new BufferedReader(new FileReader(file));
		
		String line = reader.readLine();
		System.out.println(line.subSequence(line.lastIndexOf("\"", line.length()-2)+1, line.length()-1));
		
	}

}
