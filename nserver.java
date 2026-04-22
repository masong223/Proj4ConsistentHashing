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

                        server.successorId = Integer.parseInt(messageParts[1]);
                        server.successorPort = Integer.parseInt(messageParts[2]);
                        server.predecessorId = Integer.parseInt(messageParts[3]);
                        server.predecessorPort = Integer.parseInt(messageParts[4]);
                        
                        String traversalList = messageParts[5];
                        
                        int predId = server.predecessorId;
                        int myId = server.id;

                        if (predId < myId) {
                            //Prev node + 1 to myId
                            System.out.println("Key range managed: " + (predId + 1) + ", " + myId);
                        } else if (predId > myId) {
                            //Wrapping case
                            System.out.println("Range: " + (predId + 1) + ", 1023 and 0, " + myId);
                        }
                        System.out.println("Successor ID: " + server.successorId);
                        System.out.println("Predecessor ID: " + server.predecessorId);
                        System.out.println("Servers traversed: " + traversalList);
                    } else if (respond.equalsIgnoreCase("update_successor")) {
                        server.successorId = Integer.parseInt(messageParts[1]);
                        server.successorPort = Integer.parseInt(messageParts[2]);
                        System.out.println("Updated successor to: " + server.successorId);
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

    private boolean ownId(int key) {
        if (predecessorId == id) {
            return true; // 1 node in ring
        } 
        if (predecessorId < id) { // regular case
            if (key > predecessorId && key <= id) { // key belongs to this node
                return true;
            }   else {
                return false;
            }

        } else if (predecessorId > id) { // ring case
            if (key > predecessorId || key <= id) { // key either 0 or belongs to ranges
                return true;
            } else {
                return false;
            }

        }
        return false;}

}