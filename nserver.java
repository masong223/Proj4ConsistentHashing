import java.io.*;
import java.util.*;
import java.net.*;

public class nserver {
    private TreeMap<Integer, String> localKeyData;
    private int id;
    private int port;
    private int successorId = 0;
    private int successorPort = 0;
    private int predecessorId = 0;
    private int predecessorPort = 0;

    private String bserverIp;
    private int bserverPort;

    private Socket socket = null;

    BufferedInputStream ninput = null;
    PrintWriter noutput = null;


    public nserver(String configFilePath) {
        this.localKeyData = new TreeMap<Integer, String>();
        //Set up localKeyData and init variables
        File configFile = new File(configFilePath);
        mapInit(configFile);

    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java nserver <config_file_path>");
            return;
        }
        nserver server = new nserver(args[0]);
        Scanner scanner = new Scanner(System.in);
        new Thread(() -> {
        while (true) {

            if (scanner.hasNextLine()) {
                String commandLine = scanner.nextLine();
                String[] commandParts = commandLine.split(" ");
                String command = commandParts[0];
                if (command.equalsIgnoreCase("Entry")) {
                    server.connectToBootstrap();
                    server.entry(server.socket);
                } else if (command.equalsIgnoreCase("Exit")) {
    
                }

            }

        }
        }).start();
        
        new Thread(() -> {
            // For receiving messages from servers
            try (ServerSocket serverSocket = new ServerSocket(server.port)) {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String message = input.readLine();
                    // Testing message reception
                    // We could make a method that handles different responses
                    // Entry_OK, Exit_OK, UPDATE_SUCCESSOR, etc
                    // Example in bnserver
                    String messageParts[] = message.split(" ");
                    String respond = messageParts[0];
                    if (respond.equalsIgnoreCase("Entry_OK") ) {
                        System.out.println("Entry successful");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            
        }).start();

    }

   private void mapInit(File configFile) {
        try {
            Scanner scanner = new Scanner(configFile);
            int i = 0;
            while (scanner.hasNextLine()) {
                if (i == 0) {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ");
                    this.id = Integer.parseInt(parts[0]);
                    i++;
                } else if (i == 1) {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ");
                    this.port = Integer.parseInt(parts[0]);
                    this.successorPort = port;
                    this.predecessorPort = port;
                    i++;
                } else {
                    String line = scanner.nextLine();
                    String[] parts = line.split(" ");
                    this.bserverIp = parts[0];
                    this.bserverPort = Integer.parseInt(parts[1]);
                    i++;
                }
            }
            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // helper to initiate connection to bootstrap + streams setup
    
    private void connectToBootstrap() {
         try {
            this.socket = new Socket(bserverIp, bserverPort);
            System.out.println("Connected to bnserver at " + bserverIp + ":" + bserverPort);
            this.ninput = new BufferedInputStream(socket.getInputStream());
            this.noutput = new PrintWriter(socket.getOutputStream(), true);  
        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    // helper to send entry message to bootstrap
    // Message: "Entry <id> <port>"
    private void entry(Socket socket) {
        String entryMessage = "Entry " + id + " " + port;
        noutput.println(entryMessage);
        
    }

}