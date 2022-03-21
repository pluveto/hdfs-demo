package com.less_bug.demo;

import java.io.File;
import java.util.Locale;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.beust.jcommander.JCommander;

import io.github.cdimascio.dotenv.Dotenv;

public class Main {
    private static final Logger LOGGER = Logger.getGlobal();

    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        initLogger(dotenv.get("LOG_LEVEL", "INFO"));
        System.setProperty("hadoop.home.dir", dotenv.get("HADOOP_HOME", new File("./utils").getAbsolutePath()));
        var main = new Main();
        var cliArgs = new CliArgs();
        JCommander.newBuilder()
                .addObject(cliArgs)
                .build()
                .parse(args);
        main.run(cliArgs, dotenv);
    }

    private static void initLogger(String logLevel) {

        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] [%4$-7s] %5$s %n");
        System.setProperty("user.language", "en");
        Locale.setDefault(new Locale("en", "EN"));

        logLevel = logLevel.toUpperCase();
        var level = Level.parse(logLevel);
        LOGGER.setLevel((Level) level);
        Handler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.FINE);
        LOGGER.addHandler(consoleHandler);

    }

    public void run(CliArgs cliArgs, Dotenv dotenv) {
        var client = new HdfsClient(dotenv.get("HDFS_HOST"));
        var action = cliArgs.getAction();
        LOGGER.fine("Action: " + action);
        switch (action) {
            case UPLOAD -> client.upload(cliArgs.getLocalPath(), cliArgs.getRemotePath());
            case DOWNLOAD -> client.download(cliArgs.getLocalPath(), cliArgs.getRemotePath());
            case LIST -> client.list(cliArgs.getRemoteDir());
            default -> {
                LOGGER.severe("Invalid action: " + action);
                System.exit(1);
            }
        }
    }

}
