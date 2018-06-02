package ru.bmstu.main;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import ru.bmstu.dao.DBService;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Main extends Application {
    private static final String CONFIG_FILENAME = "config.properties";

    @Override
    public void start(Stage primaryStage) throws Exception {
        loadConfig();
        Parent root = FXMLLoader.load(getClass().getResource("/form.fxml"));
        primaryStage.setTitle("MySQL автоматичекское построение индексов");
        primaryStage.setScene(new Scene(root));
        primaryStage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void stop() {
        DBService.getInstance().disconnect();
        Model.getInstance().shutdownExecutor();
    }

    private static void loadConfig() throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(CONFIG_FILENAME));
        Config.getInstance().initialize(properties);
    }
}
