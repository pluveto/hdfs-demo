package com.less_bug.demo;

import com.beust.jcommander.Parameter;

public class CliArgs {
    @Parameter(names = { "-a", "--action" }, description = "Action to perform", required = true)
    private Action action;

    @Parameter(names = { "-f", "--file" }, description = "Local path of file", required = false)
    private String localPath;

    @Parameter(names = { "-t", "--target" }, description = "Remote path of file", required = false)
    private String remotePath;

    @Parameter(names = { "-d", "--target-dir" }, description = "Remote dir", required = false)
    private String remoteDir;

    public Action getAction() {
        return action;
    }

    public String getLocalPath() {
        return localPath;
    }

    public String getRemotePath() {
        return remotePath;
    }

    public String getRemoteDir() {
        return remoteDir;
    }
}