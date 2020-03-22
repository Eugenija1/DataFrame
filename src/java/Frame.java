package java;

import javax.swing.*;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;

public class Frame extends JFrame {

    public Frame() {
        super( "DataFrame" );
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(600, 500);
        setLocation(100,50);
        setLayout(new FlowLayout());

        JFileChooser fileChooser = new JFileChooser("/Users/marinalukacik/Documents/DataFrame/src");
        fileChooser.setFileFilter(new FileNameExtensionFilter("CSV files", "csv"));
        add(fileChooser);


        ActionListener actionListener = new ActionListener() {
            public void actionPerformed(ActionEvent actionEvent) {
                JFileChooser theFileChooser = (JFileChooser) actionEvent.getSource();
                String command = actionEvent.getActionCommand();
                if (command.equals(JFileChooser.APPROVE_SELECTION)) {
                    File selectedFile = theFileChooser.getSelectedFile();
                    System.out.println(selectedFile.getParent() + "/" + selectedFile.getName());
                    setVisible(false);
                    dispose();

                    T1Data table = new T1Data(selectedFile.getParent() + "/" + selectedFile.getName());
                    T1Data.createAndShowGUI(selectedFile.getParent() + "/" + selectedFile.getName());
                } else if (command.equals(JFileChooser.CANCEL_SELECTION)) {
                    System.out.println(JFileChooser.CANCEL_SELECTION);
                }
            }
        };
        fileChooser.addActionListener(actionListener);
        setVisible(true);
    }
    public static void main(String[] args){
        Frame fr = new Frame();
    }
}
