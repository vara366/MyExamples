
package com.examples.hdfs.foldercreation;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UploadDataToHDFS {

	public static void main(String ar[]) throws IOException

	{
		String HDFS_FOLDER = ar[0];
		String LOCAL_DIRECTORY = ar[1];

		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path hdfsPath = new Path(HDFS_FOLDER);

		if (!fileSystem.exists(hdfsPath)) {
			System.out.println("Directory (" + hdfsPath + ") Existing Status = " + fileSystem.exists(hdfsPath));
			System.out.println("Creating Directory (" + hdfsPath + ") = " + fileSystem.mkdirs(hdfsPath));

			uploadFiles(conf, fileSystem, hdfsPath, LOCAL_DIRECTORY);

		} else {
			System.out.println("Directory" + hdfsPath + ") Existing Status = " + fileSystem.exists(hdfsPath));
		}

		fileSystem.close();
	}

	private static void uploadFiles(Configuration conf, FileSystem fileSystem, Path hdfsPath, String lOCAL_DIRECTORY)
			throws IOException {
		System.out.println("Uploading files from LOCAL (" + lOCAL_DIRECTORY + ") To HDFS (" + hdfsPath + ")");
		fileSystem.copyFromLocalFile(new Path(lOCAL_DIRECTORY), hdfsPath);
		System.out.println(
				"Uploading files from LOCAL (" + lOCAL_DIRECTORY + ") To HDFS (" + hdfsPath + ") process is done");

	}

}