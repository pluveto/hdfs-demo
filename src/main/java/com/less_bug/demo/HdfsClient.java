package com.less_bug.demo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class HdfsClient {

    private static final Logger LOGGER = Logger.getLogger(HdfsClient.class);

    private String host;

    public HdfsClient(String host) {
        this.host = host;
    }

    public FileSystem getFileSystem() throws IOException, InterruptedException, URISyntaxException {
        var configuration = new Configuration();
        configuration.set("fs.defaultFS", host); // "hdfs://localhost:9000"
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("dfs.client.use.datanode.hostname", "true");
        configuration.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // return FileSystem.get(configuration, "root");
        var username = "root";
        return FileSystem.get(new URI(host), configuration, username);
    }

    public void upload(String localPath, String remotePath) {
        if (localPath == null || localPath.isEmpty()) {
            LOGGER.error("localPath is null or empty");
            return;
        }
        if (remotePath == null || remotePath.isEmpty()) {
            LOGGER.error("remotePath is null or empty");
            return;
        }
        
        LOGGER.trace("Uploading file: " + localPath + " to " + remotePath);
        Path localPathObj = new Path(localPath);
        Path remotePathObj = new Path(remotePath);
        try (var fs = getFileSystem()) {
            fs.copyFromLocalFile(localPathObj, remotePathObj);
        } catch (Exception e) {
            LOGGER.fatal("Error while uploading file: " + localPath + " to " + remotePath);
            e.printStackTrace();
        }
    }

    public void download(String localPath, String remotePath) {
        if(localPath == null || localPath.isEmpty()) {
            LOGGER.fatal("Local path is empty");
            return;
        }
        if(remotePath == null || remotePath.isEmpty()) {
            LOGGER.fatal("Remote path is empty");
            return;
        }

        LOGGER.trace("Downloading file: " + localPath);
        Path localPathObj = new Path(localPath);
        Path remotePathObj = new Path(remotePath);
        try (var fs = getFileSystem()) {
            fs.copyToLocalFile(remotePathObj, localPathObj);
        } catch (Exception e) {
            LOGGER.fatal("Error while downloading file: " + localPath);
            e.printStackTrace();
        }
    }

    public void list(String remoteDir) {
        if (remoteDir == null || remoteDir.isEmpty()) {            
            System.err.println("Remote directory is not specified");
            return;
        }

        LOGGER.trace("Listing files in " + remoteDir);
        Path remotePathObj = new Path(remoteDir);
        try (var fs = getFileSystem()) {
            var status = fs.listStatus(remotePathObj);
            for (var fileStatus : status) {
                System.out.println(fileStatus.getPath().toString());
            }
        } catch (Exception e) {
            LOGGER.fatal("Error while listing files in " + remoteDir);
            e.printStackTrace();
        }
    }

    public void head(String remotePath, long size) {
        if (remotePath == null || remotePath.isEmpty()) {
            LOGGER.fatal("Remote path is empty");
            return;
        }
        if (size <= 0) {
            LOGGER.fatal("Size is not specified");
            return;
        }

        LOGGER.trace("Heading file: " + remotePath);
        Path remotePathObj = new Path(remotePath);
        try (var fs = getFileSystem()) {
            // read first size bytes
            var is = fs.open(remotePathObj);
            var buffer = new byte[(int) size];
            is.read(buffer);
            System.out.println(new String(buffer));
        } catch (Exception e) {
            LOGGER.fatal("Error while heading file: " + remotePath);
            e.printStackTrace();
        }
    }

    public void tail(String remotePath, long size) {
        if (remotePath == null || remotePath.isEmpty()) {
            LOGGER.fatal("Remote path is empty");
            return;
        }
        if (size <= 0) {
            LOGGER.fatal("Size is not specified");
            return;
        }

        LOGGER.trace("Tailing file: " + remotePath);
        Path remotePathObj = new Path(remotePath);
        try (var fs = getFileSystem()) {
            // read last size bytes
            var is = fs.open(remotePathObj);
            var buffer = new byte[(int) size];
            is.seek(is.getPos() - size);
            is.read(buffer);
            System.out.println(new String(buffer));
        } catch (Exception e) {
            LOGGER.fatal("Error while tailing file: " + remotePath);
            e.printStackTrace();
        }
    }
}
