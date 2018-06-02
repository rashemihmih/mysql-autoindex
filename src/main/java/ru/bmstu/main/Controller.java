package ru.bmstu.main;

import javafx.fxml.FXML;
import javafx.scene.control.*;

public class Controller {
    private Model model;
    private static Controller instance;

    @FXML
    private TabPane tabPane;
    @FXML
    private Tab tabInput;
    @FXML
    private Tab tabResult;
    @FXML
    private TextArea taInput;
    @FXML
    private TextArea taResult;
    @FXML
    private Button btnStart;

    public Controller() {
        instance = this;
        model = Model.getInstance();
        model.setController(this);
    }

    public static Controller getInstance() {
        return instance;
    }

    @FXML
    private void initialize() {
        model.connectDb();
    }

    @FXML
    private void calculateIndexes() {
        btnStart.setDisable(true);
        tabPane.getSelectionModel().select(tabResult);
        model.calculateIndexes(taInput.getText());
    }

    public void updateResult(String result, boolean isFinished) {
        if (isFinished) {
            btnStart.setDisable(false);
        }
        taResult.setText(result);
    }

    public void showDialog(String header, String content) {
        Dialog<String> dialog = new Dialog<>();
        dialog.getDialogPane().getButtonTypes().add(ButtonType.OK);
        dialog.setTitle("Сообщение");
        dialog.setHeaderText(header);
        dialog.setContentText(content);
        dialog.show();
    }
}
