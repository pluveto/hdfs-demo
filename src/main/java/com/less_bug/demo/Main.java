package com.less_bug.demo;

import java.io.File;
import java.util.Locale;

import com.beust.jcommander.JCommander;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import io.github.cdimascio.dotenv.Dotenv;

public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        initLogger();
        System.setProperty("hadoop.home.dir", dotenv.get("HADOOP_HOME", new File("./utils").getAbsolutePath()));
        var main = new Main();
        var cliArgs = new CliArgs();
        JCommander.newBuilder()
                .addObject(cliArgs)
                .build()
                .parse(args);
        main.run(cliArgs, dotenv);
    }

    private static void initLogger() {

        PropertyConfigurator.configure("log4j.properties");

        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] [%4$-7s] %5$s %n");
        System.setProperty("user.language", "en");
        Locale.setDefault(new Locale("en", "EN"));

    }

    public void run(CliArgs cliArgs, Dotenv dotenv) {
        var client = new HdfsClient(dotenv.get("HDFS_HOST"));
        var action = cliArgs.getAction();
        LOGGER.trace("Action: " + action);
        switch (action) {
            case UPLOAD -> client.upload(cliArgs.getLocalPath(), cliArgs.getRemotePath());
            case DOWNLOAD -> client.download(cliArgs.getLocalPath(), cliArgs.getRemotePath());
            case LIST -> client.list(cliArgs.getRemoteDir());
            case HEAD -> client.head(cliArgs.getRemotePath(), cliArgs.getLength());
            case TAIL -> client.tail(cliArgs.getRemotePath(), cliArgs.getLength());
            default -> {
                LOGGER.fatal("Invalid action: " + action);
                System.exit(1);
            }
        }
    }

}
