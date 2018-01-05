package com.wysengine.hdfs;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * HDFS工具类
 */

public class HdfsUtil {
    public static final String yearmonth = "yearmonth";
    private FileSystem fileSystem;
    private String hdfsPath = "/mywind/history/parquet";
    private String hdfsServer = "hdfs://mingyang1:8020";

    public HdfsUtil() throws URISyntaxException, IOException {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        this.fileSystem = FileSystem.newInstance(new URI(hdfsServer), new Configuration());
    }
    public void renameDir() throws IOException {
        Path hdfsFilePath = new Path(this.hdfsPath);
        for(FileStatus fs : fileSystem.listStatus(hdfsFilePath)){
            Path project = fs.getPath();
            for (FileStatus turbineFs : fileSystem.listStatus(project)) {
                for (FileStatus yearMonth : fileSystem.listStatus(turbineFs.getPath())) {
                    Path yearMonthPath = yearMonth.getPath();
                    if (!yearMonthPath.getName().contains(yearmonth)) {
                        System.out.println("rename origin = " + yearMonthPath);
                        String afterRename = yearMonthPath.getParent() + "/" + yearmonth + "=" + yearMonthPath.getName();
                        System.out.println("rename after = " + afterRename);
                        fileSystem.rename(yearMonthPath,new Path(afterRename));
                    }
                }
            }
        }
    }

    private String readFile(String hdfsPath) throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(hdfsPath));
        return IOUtils.toString(fsDataInputStream, "UTF-8");
    }

    public void moveSchema() throws IOException {
        Path hdfsFilePath = new Path(this.hdfsPath);
        for(FileStatus fs : fileSystem.listStatus(hdfsFilePath)){
            Path project = fs.getPath();
            for (FileStatus turbineFs : fileSystem.listStatus(project)) {
                Path turbinePath = turbineFs.getPath();
                for (FileStatus yearMonth : fileSystem.listStatus(turbinePath)) {
                    Path yearMonthPath = yearMonth.getPath();
                    if (yearMonthPath.getName().contains(yearmonth)) {
                        String afterRename = hdfsServer + "/mywind/history/parquetSchema/" + project.getName() + "/" + turbinePath.getName() + "/" + yearMonthPath.getName() + "/schema";
                        Path src = new Path(yearMonthPath + "/schema");
                        if (fileSystem.exists(src)) {
                            System.out.println("move after = " + afterRename);
                            System.out.println("move origin = " + (yearMonthPath + "/schema"));
                            FileUtil.copy(fileSystem, src, fileSystem, new Path(afterRename), true, new Configuration());
                        }
                    }
                }
            }
        }
    }

    public void createHiveTable() throws IOException, SQLException, ClassNotFoundException {
        Path hdfsFilePath = new Path(this.hdfsPath);
        for(FileStatus fs : fileSystem.listStatus(hdfsFilePath)){
            Path project = fs.getPath();
            for (FileStatus turbineFs : fileSystem.listStatus(project)) {
                Path turbinePath = turbineFs.getPath();
                FileStatus[] yearMonthStatuses = fileSystem.listStatus(turbinePath);
                if (yearMonthStatuses != null && yearMonthStatuses.length > 0) {
                    List<String> yearMonthList = new ArrayList<>();
                    for (FileStatus yearMonth : yearMonthStatuses) {
                        Path yearMonthPath = yearMonth.getPath();
                        if (yearMonthPath.getName().contains(yearmonth)) {
                            yearMonthList.add(yearMonthPath.getName());
                        }
                    }
                    if (CollectionUtils.isNotEmpty(yearMonthList)) {
                        String schemaPath = hdfsServer + "/mywind/history/parquetSchema" + "/" + project.getName() + "/" + turbinePath.getName() + "/" + yearMonthList.get(0) + "/schema";
                        String schema = readFile(schemaPath + "/part-00000");
                        String fields = getFields(schema);
                        HiveUtil.createTable("parquet_" + turbinePath.getName(), turbinePath.toString(), fields, yearMonthList);
                    }
                }
            }
        }
    }

    private String getFields(String schema) {
        String[] schemaArray = schema.split(System.getProperty("line.separator"));
        StringBuilder stringBuilder = new StringBuilder();
        for (String s : schemaArray) {
            stringBuilder.append(s).append(" string").append(",");
        }
        if (stringBuilder.length() > 0) {
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        }
        return stringBuilder.toString();
    }


    public static void main(String[] args) throws IOException, URISyntaxException, SQLException, ClassNotFoundException {
        HdfsUtil hdfsUtil = new HdfsUtil();
        hdfsUtil.renameDir();
        hdfsUtil.moveSchema();
        hdfsUtil.createHiveTable();
    }
}
