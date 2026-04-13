import java.io.*;
import java.util.*;

public class bnserver {
    private TreeMap<Integer, String> localKeyData;
    private int id;
    private int port;
    private int successorId = 0;
    private int predecessorId = 0;

    public bnserver(String configFilePath) {
        this.localKeyData = new TreeMap<Integer, String>();
        this.id = 0;
        this.port = 0;

        //Initially points to iteslf on creation (only 1 node)
        this.successorId = 0;
        this.predecessorId = 0;

        //Set up localKeyData
        File configFile = new File(configFilePath);
        mapInit(configFile);
    }
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java bnserver <config_file_path>");
            return;
        }
        
        bnserver server = new bnserver(args[0]);
    }

    //Set up instance variables and put all initial values into the Bootstrap Server's map
    private void mapInit(File configFile) {
        try {
            Scanner scanner = new Scanner(configFile);
            int i = 0;
            while (scanner.hasNextLine()) {
                if (i == 0) {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ");
                    id = Integer.parseInt(parts[0]);
                    i++;
                } else if (i == 1) {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ");
                    port = Integer.parseInt(parts[0]);
                    i++;
                } else {
                String line = scanner.nextLine();
                String[] parts = line.split(" ");
                int key = Integer.parseInt(parts[0]);
                String value = parts[1];
                localKeyData.put(key, value);
                i++;
            }
        }
        scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}