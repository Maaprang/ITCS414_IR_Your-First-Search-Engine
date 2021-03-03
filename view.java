import javax.swing.*;
import java.awt.*;

public class view
{
    public static void main(String[] arg)
    {
        JFrame f = new JFrame("View");
        f.setSize(250,250);
        f.getContentPane().add(BorderLayout.CENTER, new JTextArea(10,40));
        f.setVisible(true);
    }
}
