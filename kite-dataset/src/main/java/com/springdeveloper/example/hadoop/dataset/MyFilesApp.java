package com.springdeveloper.example.hadoop.dataset;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.kitesdk.data.*;
import org.kitesdk.data.filesystem.FileSystemDatasetRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

import java.io.File;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@ComponentScan
@EnableAutoConfiguration
public class MyFilesApp implements CommandLineRunner {

    private List<FileInfo> fileList = new ArrayList<FileInfo>();

	private long count;

    public static void main(String[] args) {
        SpringApplication.run(MyFilesApp.class, args);
    }

    private DatasetWriter writer;

    @Override
    public void run(String... strings) throws Exception {
        String user = System.getProperty("user.name");
        String path = "/user/" + user + "/demo";
        String homeDir = System.getProperty("user.home");
        System.out.println("Reading from local " + homeDir);
        System.out.println("Writing to HDFS " + path);
        init(path);
		File f = new File(homeDir);
		processFile(f);
		writeFileList();
        writer.close();
		System.out.println("Done!");
    }

	private void init(String path) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:8020");
        DatasetRepository datasetRepository = new FileSystemDatasetRepository.Builder()
                .rootDirectory(new URI(path)).configuration(conf).build();
        Schema schema = ReflectData.get().getSchema(FileInfo.class);
//        PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash("year", "HASH", 5).build();
        PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().identity("year", "ID", 10).build();
        DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
                .schema(schema)
                .format(Formats.AVRO)
                .partitionStrategy(partitionStrategy)
                .build();
        Dataset<FileInfo> dataset = datasetRepository.create("fileinfo", descriptor);
        writer = dataset.newWriter();
        System.out.println("** USING: " + writer);
        writer.open();
    }

	private void processFile(File file) {
		if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				processFile(f);
			}
		} else {
			SimpleDateFormat sdf = new SimpleDateFormat("YYYY");
			String year = sdf.format(new Date(file.lastModified())).substring(3,4);
			fileList.add(new FileInfo(file.getParent(), file.getName(), year, file.length(), file.lastModified()));
			if (fileList.size() >= 100000) {
				writeFileList();
			}
		}
	}

	private void writeFileList() {
		count = count + fileList.size();
		System.out.println("Writing " + count + " ...");
        for (FileInfo f : fileList) {
            writer.write(f);
        }
        writer.flush();
        fileList.clear();
	}

}
