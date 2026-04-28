import java.io.*;
import java.lang.*;
import java.net.*;
import java.util.*;

public class bnserver {
    private TreeMap<Integer, String> localKeyData; // Sorts automatically by key, which is helpful for deciding where to
                                                   // store keys without extra steps
    private int id;
    private int port;
    private int successorId = 0;
    private int successorPort = 0;
    private int predecessorId = 0;
    private int predecessorPort = 0;

    private String successorIp;
    private String predecessorIp;
    private String myIp;

    private Socket socket = null;

    public bnserver(String configFilePath) {
        this.localKeyData = new TreeMap<Integer, String>();
        this.id = 0;
        this.port = 0;

        try {
            this.myIp = InetAddress.getLocalHost().getHostAddress();
            this.successorIp = this.myIp; // Initially points to itself
            this.predecessorIp = this.myIp;
        } catch (UnknownHostException e) {
            this.myIp = "127.0.0.1";
        }

        // Initially points to iteslf on creation (only 1 node)
        this.successorId = 0;
        this.predecessorId = 0;

        // Set up localKeyData
        File configFile = new File(configFilePath);
        mapInit(configFile);

    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java bnserver <config_file_path>");
            return;
        }
        bnserver server = new bnserver(args[0]);
        Scanner scanner = new Scanner(System.in);

        // Handles user commands (Lookup, Insert, Delete)
        new Thread(() -> {
            while (true) {
                System.out.print("Enter command > ");
                String commandLine = scanner.nextLine();
                server.userCommand(commandLine);
            }

        }).start();

        // Handles name servers messaging (Entry, Exit)
        new Thread(() -> {
            // name server commands
            try (ServerSocket serverSocket = new ServerSocket(server.port)) {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String message = input.readLine();
                    server.serverCommand(message, clientSocket, server , input);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }).start();
    }

    // Set up instance variables and put all initial values into the Bootstrap
    // Server's map
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
                    successorPort = port; // Initially points to itself
                    predecessorPort = port; // Initially points to itself
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

    // helper to process user Command (Lookup, Insert, Delete)
    private void userCommand(String commandLine) {

        String[] commandParts = commandLine.split(" ");
        String command = commandParts[0];
        if (command.equalsIgnoreCase("Lookup")) {
            if (commandParts.length < 2) {
                System.out.println("Lookup command requires a key");
                return;
            }
            int key;
            try {
                key = Integer.parseInt(commandParts[1]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid key format. Key should be an integer.");
                return;
            }
            if (key > predecessorId || key == 0 || predecessorId == 0 && successorId == 0) {
                String value = localKeyData.get(key);
                if (value != null) {
                    System.out.println("Lookup result: " + value + " found at server " + id + ". Traversal: " + id);
                } else {
                    System.out.println("Key not found." + ". Traversal: " + id);
                }
            } else {
                try {
                    Socket successorSocket = new Socket(successorIp, successorPort);
                    PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                    output.println(command + " " + key + " 0"); // the "0" is the beginning of the traversal list.
                    BufferedReader input = new BufferedReader(new InputStreamReader(successorSocket.getInputStream()));
                    String response = input.readLine();
                    System.out.println("Lookup " + key + " " + response);
                    successorSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        else if (command.equalsIgnoreCase("Insert")) {
            if (commandParts.length < 3) {
                System.out.println("Insert command requires a key and a value");
                return;
            }
            try {
                int key;
                String value = commandParts[2];
                try {
                    key = Integer.parseInt(commandParts[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid key format. Key should be an integer.");
                    return;
                }
                if (key > predecessorId || predecessorId == 0 && successorId == 0) {
                    localKeyData.put(key, commandParts[2]);
                    System.out.println("Inserted key: " + key + ", value: " + commandParts[2] + " into server " + id + ". Traversal: " + id);
                } else {
                    try {
                        Socket successorSocket = new Socket(successorIp, successorPort);
                        PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                        output.println(command + " " + key + " " + value + " 0"); // the "0" is the beginning of the traversal list.
                        BufferedReader input = new BufferedReader(
                                new InputStreamReader(successorSocket.getInputStream()));
                        String response = input.readLine();
                        System.out.println("Inserted " + "(" + key + ", " + value + ")" + response);
                        successorSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            } catch (NumberFormatException e) {
                System.out.println("Invalid key format. Key should be an integer.");
            }
        } else if (command.equalsIgnoreCase("Delete")) {
            if (commandParts.length < 2) {
                System.out.println("Delete command requires a key");
                return;
            }
            try {
                int key = Integer.parseInt(commandParts[1]);
                if (key > predecessorId || key == 0 || predecessorId == 0 && successorId == 0) {
                    String removedValue = localKeyData.remove(key);
                    if (removedValue != null) {
                        System.out.println("Deleted key: " + key + ", value: " + removedValue + " from server " + id);
                    } else {
                        System.out.println("Key not found. Nothing deleted.");
                    }
                } else {
                    try {
                        Socket successorSocket = new Socket(successorIp, successorPort);
                        PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                        output.println(command + " " + key + " 0"); // the "0" is the beginning of the traversal list.
                        BufferedReader input = new BufferedReader(new InputStreamReader(successorSocket.getInputStream()));
                        String response = input.readLine();
                        System.out.println("Successful Deletion of key: " + key + " " + response);
                        successorSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            } catch (NumberFormatException e) {
                System.out.println("Invalid key format. Key should be an integer.");
            }

        } else if (command.equalsIgnoreCase("info")) {
            System.out.println("Node ID: " + id);
            System.out.println("Port: " + port);
            System.out.println("Predecessor ID: " + predecessorId);
            System.out.println("Predecessor Port: " + predecessorPort);
            System.out.println("Successor ID: " + successorId);
            System.out.println("Successor Port: " + successorPort);
        } else if (command.equalsIgnoreCase("Delete")) {
            if (commandParts.length < 2) {
                System.out.println("Delete command requires a key");
                return;
            }
            try {
                int key = Integer.parseInt(commandParts[1]);

            } catch (NumberFormatException e) {
                System.out.println("Invalid key format. Key should be an integer.");
            }
        } else {
            System.out.println("Invalid command. Please use Lookup, Insert, or Delete.");
        }
    }

    // helper to process commands from name server (Entry, Exit)
    private void serverCommand(String message, Socket socket, bnserver server, BufferedReader input ) {
        System.out.println("Received message from name server: " + message);
        String commandParts[] = message.split(" ");
        String command = commandParts[0];
        boolean inTransfer = false;
        if (command.equalsIgnoreCase("Entry")) {
            System.out.println("Received Entry command from name server.");
            int newNodeId = Integer.parseInt(commandParts[1]);
            String newNodeIp = commandParts[2];
            int newNodePort = Integer.parseInt(commandParts[3]);
            String traversalList = "";
            if (commandParts.length > 4) {
                traversalList = commandParts[4];
            }
            if (ownId(newNodeId) == true) { // Id in bootServer's range
                String idTravseralListFinal = traversalList.isEmpty() ? String.valueOf(id) : traversalList + "," + id;
                if (predecessorId == id && successorId == id) { // Only 1 node in ring
                    // Send message to new node with succesor/predecessor info
                    // BootServer is only server in ring
                    try {
                        Socket newNodeSocket = new Socket(newNodeIp, newNodePort);
                        PrintWriter output = new PrintWriter(newNodeSocket.getOutputStream(), true);

                        String entryRespondMessage = "ENTRY_OK " + id + " " + myIp + " " + port + " " + id + " " + myIp + " " + port + " " + idTravseralListFinal;
                        // Message: "Entry_OK <id> <port> <successorId> <successorPort>"
                        output.println(entryRespondMessage);
                        // Update successor/predecessor info
                        successorId = newNodeId;
                        successorPort = newNodePort;
                        successorIp = newNodeIp;
                        predecessorId = newNodeId;
                        predecessorPort = newNodePort;
                        predecessorIp = newNodeIp;
                        newNodeSocket.close();
                        System.out.println(newNodeSocket.isClosed());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    // Case when entering server's ID is in the bootserver's range.
                    try {

                        Socket newNodeSocket = new Socket(newNodeIp, newNodePort);
                        PrintWriter output = new PrintWriter(newNodeSocket.getOutputStream(), true);
                        String entryMessage = "ENTRY_OK " + id + " " + myIp + " " + port + " " + predecessorId + " " + predecessorIp + " " + predecessorPort + " " + idTravseralListFinal;
                        // Message: "ENTRY_OK <succesorId> <successorPort> <predecessorId>
                        // <predecessorPort>"
                        output.println(entryMessage);

                        // Send message to predecessor to update its info
                        Socket oldPredecessorSocket = new Socket(predecessorIp, predecessorPort);
                        PrintWriter oldPredecessorOutput = new PrintWriter(oldPredecessorSocket.getOutputStream(), true);
                        String updatePredecessorMessage = "update_successor " + newNodeId + " " + newNodeIp + " " + newNodePort;
                        oldPredecessorOutput.println(updatePredecessorMessage);
                        //Update info
                        predecessorId = newNodeId;
                        predecessorIp = newNodeIp;
                        predecessorPort = newNodePort;
                        newNodeSocket.close();
                        oldPredecessorSocket.close();
                        System.out.println(newNodeSocket.isClosed() + "range in boot");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                // Forward message to succesor
                try {
                    Socket successorSocket = new Socket(successorIp, successorPort);
                    PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);

                    // If the traversal list is empty, add the node id. If it's not empty, append id
                    // onto it. Separated by commas to avoid " " splitting
                    String updatedTraversalList = traversalList.isEmpty() ? String.valueOf(id)
                            : traversalList + "," + id;
                    String messageToForward = "Entry " + newNodeId + " " + newNodeIp + " " + newNodePort + " " + updatedTraversalList;
                    output.println(messageToForward);
                    successorSocket.close();
                    // Probably add header to track which servers have seen this message
                    // <message> <> <> <>
                    // <tracker> <id1, id2, id3....>
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
     
        } else if (command.equalsIgnoreCase("Request")) {
            // data is requested by new predeccessor, we use an iterator to dynamically remove data from the TreeMap
            try {
                Socket predecessorSocket = new Socket(predecessorIp, predecessorPort);
                PrintWriter output = new PrintWriter(predecessorSocket.getOutputStream(), true);
                output.println("Sending_data");
                Iterator<Map.Entry<Integer, String>> iterator = localKeyData.entrySet().iterator();

                while (iterator.hasNext()) {
                    Map.Entry<Integer, String> entry = iterator.next();

                    System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());

                    if (entry.getKey() > predecessorId || entry.getKey() == 0) {
                        System.out.println("We do not transfer " + entry.getKey() + " " + entry.getValue());
                        if (entry.getKey() != 0) {
                            output.println("End_data");
                            break;
                        }
                    } else {
                        output.println(entry.getKey() + " " + entry.getValue());
                        iterator.remove(); // safe removal
                    }
                } 
                predecessorSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        } else if (command.equalsIgnoreCase("Key") || command.equalsIgnoreCase("Lookup")
                || command.equalsIgnoreCase("Insert") || command.equalsIgnoreCase("Successful")) {
            System.out.println(message);
        } else if (command.equalsIgnoreCase("update_successor")) {
            int newSuccessorId = Integer.parseInt(commandParts[1]);
            int newSuccessorPort = Integer.parseInt(commandParts[2]);
            successorId = newSuccessorId;
            successorPort = newSuccessorPort;
            System.out.println("Updated successor to: " + successorId);

        } else if (command.equalsIgnoreCase("Sending_data")) {
                // We receive sending_data from node, we parse every value pair/key until read End_data
                try {
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
                } catch (IOException e) {
                    e.printStackTrace();
                } 
                
    
            
        }else if (command.equalsIgnoreCase("Exit")) {
                int exitingNodeId = Integer.parseInt(commandParts[1]);
                String exitingNodeIp = commandParts[2];
                int exitingNodePort = Integer.parseInt(commandParts[3]);
                
                int exitingSuccessorId = Integer.parseInt(commandParts[4]);
                String exitingSuccessorIp = commandParts[5];
                int exitingSuccessorPort = Integer.parseInt(commandParts[6]);
                
                int exitingPredecessorId = Integer.parseInt(commandParts[7]);
                String exitingPredecessorIp = commandParts[8];
                int exitingPredecessorPort = Integer.parseInt(commandParts[9]);

                try {
                    if (exitingNodeId == server.successorId) {
                        System.out.println("exiting node is my successor");

                        successorId = exitingSuccessorId;
                        successorPort = exitingSuccessorPort;
                        successorIp = exitingSuccessorIp;

                        Socket successorSocket = new Socket(successorIp, successorPort);
                        PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                        output.println(message);
                        successorSocket.close();

                    } else if (exitingNodeId == predecessorId) {
                        System.out.println("exiting node is my predecessor");

                        Socket exitingSocket = new Socket(exitingNodeIp, exitingNodePort);
                        PrintWriter output = new PrintWriter(exitingSocket.getOutputStream(), true);
                        output.println("request_data");
                        exitingSocket.close();

                        predecessorId = exitingPredecessorId;
                        predecessorPort = exitingPredecessorPort;
                        predecessorIp = exitingPredecessorIp;

                    } else {
                        Socket successorSocket = new Socket(successorIp, successorPort);
                        PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                        output.println(message);
                        successorSocket.close();
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
               
        }  
        else {
            System.out.println("Invalid command from name server. Please use Entry or Exit.");
        }

        // Process the message/command from the name server

    }

    // check if Id is in BootServers'range
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
