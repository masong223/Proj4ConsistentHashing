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

    Socket socket = null;

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
                } 
                else if (command.equalsIgnoreCase("Exit")) {
    
                } else if (command.equalsIgnoreCase("info")) {
                    System.out.println("Node ID: " + server.id);
                    System.out.println("Port: " + server.port);
                    System.out.println("Predecessor ID: " + server.predecessorId);
                    System.out.println("Predecessor Port: " + server.predecessorPort);
                    System.out.println("Successor ID: " + server.successorId);
                    System.out.println("Successor Port: " + server.successorPort);
                }


            }

        }
        }).start();
        
        new Thread(() -> {
            // For receiving messages from servers
            try (ServerSocket serverSocket = new ServerSocket(server.port)) {
                boolean inTransfer = false;
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String message = input.readLine();
                    String messageParts[] = message.split(" ");
                    String respond = messageParts[0];
                    if (respond.equalsIgnoreCase("Entry_OK") ) {
                        System.out.println("Entry successful");
 
                        server.successorId = Integer.parseInt(messageParts[1]);
                        server.successorPort = Integer.parseInt(messageParts[2]);
                        server.predecessorId = Integer.parseInt(messageParts[3]);
                        server.predecessorPort = Integer.parseInt(messageParts[4]);
                        
                        String traversalList = messageParts[5];

                        System.out.println("Key range managed: " + (server.predecessorId + 1) + ", " + server.id);
                        
                        System.out.println("Successor ID: " + server.successorId);
                        System.out.println("Predecessor ID: " + server.predecessorId);
                        System.out.println("Servers traversed: " + traversalList);
                        clientSocket.close();
                        Socket successorSocket = new Socket("Localhost", server.successorPort);
                        PrintWriter successorOut = new PrintWriter(successorSocket.getOutputStream(), true);
                        successorOut.println("Request");
                        successorSocket.close();   
                    } else if (respond.equalsIgnoreCase("update_successor")) {
                        server.successorId = Integer.parseInt(messageParts[1]);
                        server.successorPort = Integer.parseInt(messageParts[2]);
                        System.out.println("Updated successor to: " + server.successorId);
                    } else if (respond.equals("Entry")) {
                        int newNodeId = Integer.parseInt(messageParts[1]);
                        int newNodePort = Integer.parseInt(messageParts[2]);
                        String traversalList = messageParts[3];
                        
                        if (server.ownId(newNodeId)) {
                            //transfer keys to new node
                            
                            Socket transferSocket = new Socket("Localhost", newNodePort);
                            PrintWriter output = new PrintWriter(transferSocket.getOutputStream(), true);
                            
                            String response = "Entry_OK " + server.id + " " + server.port + " " + server.predecessorId + " " + server.predecessorPort + " " + traversalList + "," + server.id;

                            Socket predecessorSocket = new Socket("Localhost", server.predecessorPort);
                            PrintWriter predecessorUpdater = new PrintWriter(predecessorSocket.getOutputStream(), true);

                            predecessorUpdater.println("update_successor " + newNodeId + " " + newNodePort);
                           // this is to the new node so they update
                            output.println(response);
                            // Going to the same port?
                            transferSocket.close();
                            predecessorSocket.close();
                            server.predecessorId = newNodeId;
                            server.predecessorPort = newNodePort;
                            clientSocket.close();
                            System.out.println("New node " + newNodeId + " has entered. Updated predecessor to: " + server.predecessorId);
                        
                        } else {
                            // Forward the entry message to the successor
                            Socket successorSocket = new Socket("Localhost", server.successorPort);
                            PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                            String updatedTraversal = traversalList + "," + server.id;
                            output.println("Entry " + newNodeId + " " + newNodePort + " " + updatedTraversal);
                            successorSocket.close();
                        }
                    } else if (respond.equalsIgnoreCase("Request")) {
                        try {
                                Socket successorSocket = new Socket("LocalHost", server.successorPort);
                                PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                                output.println("Sending_data");
                                for (Map.Entry<Integer, String> entry : server.localKeyData.entrySet()) {
                                    System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
                                    if (entry.getKey() >= server.successorId) {
                                        System.out.println("We do not transfer" + entry.getKey() + entry.getValue());
                                        output.println("End_data");
                                        break;
                                
                                    } else {
                                        output.println(entry.getKey() + entry.getValue());
                                        // Key , Value
                                    }
                                    }
                                successorSocket.close();
                                } catch (IOException e) {
                                e.printStackTrace();
                                }
                            

                    } else if (respond.equalsIgnoreCase("Sending_data")) {
                        inTransfer = true;
                        System.out.println("Receiving key transfer...");
                        while(inTransfer) {
                            String dataLine = input.readLine();
                            System.out.println("Received data: " + dataLine);
                            if (input.readLine().equalsIgnoreCase("End_data")) {
                                System.out.println("Key transfer complete");
                                inTransfer = false;
                                break;
                            }
                            
                            
                        }

                    
                    } else if (inTransfer && respond.equalsIgnoreCase("KeyTransfer:")) {
                        int key = Integer.parseInt(messageParts[1]);
                        String value = messageParts[2];
                        server.localKeyData.put(key, value);
                        System.out.println("Received key transfer: " + key + " -> " + value);
                    } else if (respond.equalsIgnoreCase("KeyTransferEnd")) {
                        System.out.println("Key transfer complete");
                        inTransfer = false;
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