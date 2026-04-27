import java.io.*;
import java.net.*;
import java.util.*;

public class nserver {
    private TreeMap<Integer, String> localKeyData;
    private int id;
    private int port;
    private int successorId = 0;
    private int successorPort = 0;
    private int predecessorId = 0;
    private int predecessorPort = 0;

    private String successorIp;
    private String predecessorIp;
    private String myIp;

    private String bserverIp;
    private int bserverPort;

    Socket socket = null;

    BufferedInputStream ninput = null;
    PrintWriter noutput = null;

    public nserver(String configFilePath) {
        this.localKeyData = new TreeMap<Integer, String>();
        // Set up localKeyData and init variables
        try {
        //grab our own ip
        this.myIp = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            this.myIp = "127.0.0.1";
        }
        
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
                    if (command.equalsIgnoreCase("enter")) {
                        server.connectToBootstrap();
                        server.entry(server.socket);
                        
                    } else if (command.equalsIgnoreCase("Exit")) {
                        server.connectToBootstrap();
                        server.exit(server.socket);

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
                    System.out.println("Received message: " + message);
                    if (respond.equalsIgnoreCase("Entry_OK")) {
                        System.out.println("Entry successful");
 
                      

                        server.successorId = Integer.parseInt(messageParts[1]);
                        server.successorIp = messageParts[2];
                        server.successorPort = Integer.parseInt(messageParts[3]);
    
                        server.predecessorId = Integer.parseInt(messageParts[4]);
                        server.predecessorIp = messageParts[5];
                        server.predecessorPort = Integer.parseInt(messageParts[6]);

                        String traversalList = messageParts[7];

                        System.out.println("Key range managed: " + (server.predecessorId + 1) + ", " + server.id);

                        System.out.println("Successor ID: " + server.successorId);
                        System.out.println("Predecessor ID: " + server.predecessorId);
                        System.out.println("Servers traversed: " + traversalList);
                        clientSocket.close();
                        Socket successorSocket = new Socket(server.successorIp, server.successorPort);
                        PrintWriter successorOut = new PrintWriter(successorSocket.getOutputStream(), true);
                        successorOut.println("Request");
                        successorSocket.close();   
                    } else if (respond.equalsIgnoreCase("update_successor")) {
                        server.successorId = Integer.parseInt(messageParts[1]);
                        server.successorIp = messageParts[2];
                        server.successorPort = Integer.parseInt(messageParts[3]);

                        System.out.println("Updated successor to: " + server.successorId);
                    } else if (respond.equals("Entry")) {
                        int newNodeId = Integer.parseInt(messageParts[1]);
                        String newNodeIp = messageParts[2];
                        int newNodePort = Integer.parseInt(messageParts[3]);
                        String traversalList = messageParts[4];

                        if (server.ownId(newNodeId)) {
                            // transfer keys to new node

                            Socket transferSocket = new Socket(newNodeIp, newNodePort);
                            PrintWriter output = new PrintWriter(transferSocket.getOutputStream(), true);
                            
                            String response = "Entry_OK " + server.id + " " + server.myIp + " " + server.port + " " + server.predecessorId + " " + server.predecessorIp + " " + server.predecessorPort + " " + traversalList + "," + server.id;
                            Socket predecessorSocket = new Socket(server.predecessorIp, server.predecessorPort);
                            PrintWriter predecessorUpdater = new PrintWriter(predecessorSocket.getOutputStream(), true);

                            predecessorUpdater.println("update_successor " + newNodeId + " " + newNodeIp + " " + newNodePort);  // this is to the new node so they update
                            output.println(response);
                            // Going to the same port?
                            transferSocket.close();
                            predecessorSocket.close();
                            server.predecessorId = newNodeId;
                            server.predecessorPort = newNodePort;
                            server.predecessorIp = newNodeIp;
                            clientSocket.close();
                            System.out.println("New node " + newNodeId + " has entered. Updated predecessor to: " + server.predecessorId);
                        
                        } else {
                            // Forward the entry message to the successor
                            Socket successorSocket = new Socket(server.successorIp, server.successorPort);
                            PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                            String updatedTraversal = traversalList + "," + server.id;
                            output.println("Entry " + newNodeId + " " + newNodeIp + " " + newNodePort + " " + updatedTraversal);
                            successorSocket.close();
                        }
                    } else if (respond.equalsIgnoreCase("Request")) {
                        try {
                                Socket predecessorSocket = new Socket(server.predecessorIp, server.predecessorPort);
                                PrintWriter output = new PrintWriter(predecessorSocket.getOutputStream(), true);
                                output.println("Sending_data");
                                Iterator<Map.Entry<Integer, String>> iterator = server.localKeyData.entrySet().iterator();

                                while (iterator.hasNext()) {
                                    Map.Entry<Integer, String> entry = iterator.next();

                                    System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());

                                    if (entry.getKey() > server.predecessorId) {
                                        System.out.println("We do not transfer " + entry.getKey() + " " + entry.getValue());
                                        output.println("End_data");
                                        break;
                                    } else {
                                        output.println(entry.getKey() + " " + entry.getValue());
                                        iterator.remove(); // safe removal
                                    }
                                } 
                                output.println("End_data");

                                predecessorSocket.close();
                                } catch (IOException e) {
                                     e.printStackTrace();
                                }
                            

                    } else if (respond.equalsIgnoreCase("Sending_data")) {
                        inTransfer = true;
                        System.out.println("Receiving key transfer...");
                        while(inTransfer) {
                            String dataLine = input.readLine();
                            
                            System.out.println("Received data: " + dataLine);
                            if (dataLine.equalsIgnoreCase("End_data")) {
                                System.out.println("Key transfer complete");
                                inTransfer = false;
                                break;
                            } else {
                                String[] dataParts = dataLine.split(" ");
                                int key = Integer.parseInt(dataParts[0]);
                                String value = dataParts[1];
                                server.localKeyData.put(key, value);
                                System.out.println("Added key: " + key + ", value: " + value + " to server " + server.id);
                            }
                            
                        }

                    
                    } else if (respond.equalsIgnoreCase("Lookup")) {
                        int key = Integer.parseInt(messageParts[1]);
                        if (key > server.predecessorId && key <= server.id) {
                            String value = server.localKeyData.get(key);
                            String result;
                            if (value != null) {
                                result = "Lookup result: " + value + " (found at node " + server.id + ")" + " Traversal: " + messageParts[2] + "," + server.id;
                            } else {
                                System.out.println("Key not found.");
                                result = "Key not found. Traversal: " + messageParts[2] + "," + server.id;
                            }
                            PrintWriter outputToClient = new PrintWriter(clientSocket.getOutputStream(), true);
                            outputToClient.println(result);
                        } else {
                            try {
                                Socket successorSocket = new Socket(server.successorIp, server.successorPort);
                                PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                                output.println(message + "," + server.id); 
                                BufferedReader inputFromSuccessor = new BufferedReader(
                                        new InputStreamReader(successorSocket.getInputStream()));
                                String response = inputFromSuccessor.readLine();
                                PrintWriter outputToClient = new PrintWriter(clientSocket.getOutputStream(), true);
                                outputToClient.println(response);
                                successorSocket.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    } else if (respond.equalsIgnoreCase("Insert")) {
                        int key = Integer.parseInt(messageParts[1]);
                        String value = messageParts[2];
                        if (key > server.predecessorId && key <= server.id) {
                            server.localKeyData.put(key, value);
                            System.out.println("Inserted key: " + key + ", value: " + value + " into server " + server.id);
                            PrintWriter outputToClient = new PrintWriter(clientSocket.getOutputStream(), true);
                            outputToClient.println("Insert successful at node " + server.id + ". Traversal: " + messageParts[3] + "," + server.id);
                        } else {
                            try {
                                Socket successorSocket = new Socket(server.successorIp, server.successorPort);
                                PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                                output.println(message + "," + server.id); 
                                BufferedReader inputFromSuccessor = new BufferedReader(
                                        new InputStreamReader(successorSocket.getInputStream()));
                                String response = inputFromSuccessor.readLine();
                                PrintWriter outputToClient = new PrintWriter(clientSocket.getOutputStream(), true);
                                outputToClient.println(response);
                                successorSocket.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    } else if (respond.equalsIgnoreCase("Delete")) {
                        int key = Integer.parseInt(messageParts[1]);
                        if (key > server.predecessorId && key <= server.id) {
                            String removedValue = server.localKeyData.remove(key);
                            System.out.println("Deleted key: " + key + " from server " + server.id);
                            PrintWriter outputToClient = new PrintWriter(clientSocket.getOutputStream(), true);
                            if (removedValue != null) {
                                outputToClient.println("Successful Deletion at node " + server.id + ". Traversal: " + messageParts[2] + "," + server.id);
                            } else {
                                outputToClient.println("Key not found for deletion. Traversal: " + messageParts[2] + "," + server.id);
                            }
                        } else {
                            try {
                                Socket successorSocket = new Socket(server.successorIp, server.successorPort);
                                PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                                output.println(message + "," + server.id); 
                                BufferedReader inputFromSuccessor = new BufferedReader(
                                        new InputStreamReader(successorSocket.getInputStream()));
                                String response = inputFromSuccessor.readLine();
                                PrintWriter outputToClient = new PrintWriter(clientSocket.getOutputStream(), true);
                                outputToClient.println(response);
                                successorSocket.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    } else if (respond.equalsIgnoreCase("Exit")) {
                        int exitingNodeId = Integer.parseInt(messageParts[1]);
                        String exitingNodeIp = messageParts[2];
                        int exitingNodePort = Integer.parseInt(messageParts[3]);
                        
                        int newSuccessorId = Integer.parseInt(messageParts[4]);
                        String newSuccessorIp = messageParts[5];
                        int newSuccessorPort = Integer.parseInt(messageParts[6]);
                        
                        int newPredecessorId = Integer.parseInt(messageParts[7]);
                        String newPredecessorIp = messageParts[8];
                        int newPredecessorPort = Integer.parseInt(messageParts[9]);

                        if (exitingNodeId == server.successorId) {
                            System.out.println("exiting node is my successor");
                            Socket successorSocket = new Socket(server.successorIp, server.successorPort);
                            PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                            server.successorId = newSuccessorId;
                            server.successorPort = newSuccessorPort;  
                            server.successorIp = newSuccessorIp; 
                            output.println(message);
                            successorSocket.close();
                            System.out.println("New successor: " + server.successorId);

                        } else if (exitingNodeId == server.predecessorId) {
                            System.out.println("exiting node is my predecessor");
                            Socket exitingSocket = new Socket(exitingNodeIp, exitingNodePort);
                            PrintWriter output = new PrintWriter(exitingSocket.getOutputStream(), true);
                            String request = "request_data"; // signal to predecessor to send keys that belong to me
                            output.println(request);
                            exitingSocket.close();
                            server.predecessorId = newPredecessorId;
                            server.predecessorPort = newPredecessorPort;
                            server.predecessorIp = newPredecessorIp;
                            System.out.println("New predecessor: " + server.predecessorId);


                        } else {
                            Socket successorSocket = new Socket(server.successorIp, server.successorPort);
                            PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                            output.println(message);
                            successorSocket.close();
                        }
                    } else if (respond.equalsIgnoreCase("request_data")) {
                            Socket successorSocket = new Socket(server.successorIp, server.successorPort);
                            PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                            Iterator<Map.Entry<Integer, String>> iterator = server.localKeyData.entrySet().iterator();
                            output.println("sending_data");
                                while (iterator.hasNext()) {
                                    Map.Entry<Integer, String> entry = iterator.next();

                                    System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
                                    output.println(entry.getKey() + " " + entry.getValue());
                                    iterator.remove(); // safe removal
                                }
                                 
                                output.println("End_data");
                                successorSocket.close();

                                System.out.println("Successful exit");
                                System.out.println("Handed over key range " + (server.predecessorId + 1) + "-" + server.id + " to Successor ID: " + server.successorId);
                                System.exit(0);
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
        String entryMessage = "Entry " + id + " " + myIp + " " + port;
        noutput.println(entryMessage);

    }

    private void exit(Socket socket) {
        String exitMessage = "Exit " + id + " " + myIp + " " + port + " " + successorId + " " + successorIp + " " + successorPort + " " + predecessorId + " " + predecessorIp + " " + predecessorPort; 
        noutput.println(exitMessage);

    }

    private boolean ownId(int key) {
        if (predecessorId == id) {
            return true; // 1 node in ring
        }
        if (predecessorId < id) { // regular case
            if (key > predecessorId && key <= id) { // key belongs to this node
                return true;
            } else {
                return false;
            }

        } else if (predecessorId > id) { // ring case
            if (key > predecessorId || key <= id) { // key either 0 or belongs to ranges
                return true;
            } else {
                return false;
            }

        }
        return false;
    }

}