package com.less_bug.demo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsClient {

    private static final Logger LOGGER = Logger.getGlobal();

    private String host;

    public HdfsClient(String host) {
        this.host = host;
    }

    public FileSystem getFileSystem() throws IOException, InterruptedException, URISyntaxException {
        var configuration = new Configuration();
        configuration.set("fs.defaultFS", host); // "hdfs://localhost:9000"
        configuration.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // return FileSystem.get(configuration, "root");
        var username = "root";
        return FileSystem.get(new URI(host), configuration, username);
    }

    public void upload(String localPath, String remotePath) {
        LOGGER.fine("Uploading file: " + localPath + " to " + remotePath);
        Path localPathObj = new Path(localPath);
        Path remotePathObj = new Path(remotePath);
        try (var fs = getFileSystem()) {
            fs.copyFromLocalFile(localPathObj, remotePathObj);
        } catch (Exception e) {
            LOGGER.severe("Error while uploading file: " + localPath + " to " + remotePath);
            e.printStackTrace();
        }
    }

    public void download(String localPath, String remotePath) {
        LOGGER.fine("Downloading file: " + localPath);
        Path localPathObj = new Path(localPath);
        Path remotePathObj = new Path(remotePath);
        try (var fs = getFileSystem()) {
            fs.copyToLocalFile(remotePathObj, localPathObj);
        } catch (Exception e) {
            LOGGER.severe("Error while downloading file: " + localPath);
            e.printStackTrace();
        }
    }

    public void list(String remoteDir) {
        LOGGER.fine("Listing files in " + remoteDir);
        Path remotePathObj = new Path(remoteDir);
        try (var fs = getFileSystem()) {
            var status = fs.listStatus(remotePathObj);
            for (var fileStatus : status) {
                LOGGER.info(fileStatus.getPath().toString());
            }
        } catch (Exception e) {
            LOGGER.severe("Error while listing files in " + remoteDir);
            e.printStackTrace();
        }
    }

}
