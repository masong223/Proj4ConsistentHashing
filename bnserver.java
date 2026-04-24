import java.io.*;
import java.util.*;
import java.net.*;
import java.lang.*;

public class bnserver {
    private TreeMap<Integer, String> localKeyData; //Sorts automatically by key, which is helpful for deciding where to store keys without extra steps
    private int id;
    private int port;
    private int successorId = 0;
    private int successorPort = 0;
    private int predecessorId = 0;
    private int predecessorPort = 0;
    private Socket socket = null;
    
    
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
        Scanner scanner = new Scanner(System.in);

        // Handles user commands (Lookup, Insert, Delete)
        new Thread(() -> {
            // client commands
            while (true) {
                String commandLine = scanner.nextLine();
                server.userCommand(commandLine);
            }

        }).start();

        // Handles name servers messaging (Entry, Exit)
        new Thread(() -> {
            // name server commands
            try(ServerSocket serverSocket = new ServerSocket(server.port)) {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    String message = input.readLine();
                    server.serverCommand(message, clientSocket, server);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }).start();
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
                    successorPort = port; //Initially points to itself
                    predecessorPort = port; //Initially points to itself
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
                } else {
                    try {
                        int key = Integer.parseInt(commandParts[1]);
                        
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid key format. Key should be an integer.");
                    }
                }
                

            } else if (command.equalsIgnoreCase("Insert")) {


            } else if (command.equalsIgnoreCase("Delete")) {

            } else if (command.equalsIgnoreCase("info")) {
                    System.out.println("Node ID: " + id);
                    System.out.println("Port: " + port);
                    System.out.println("Predecessor ID: " + predecessorId);
                    System.out.println("Predecessor Port: " + predecessorPort);
                    System.out.println("Successor ID: " + successorId);
                    System.out.println("Successor Port: " + successorPort);
            } else {

            System.out.println("Invalid command. Please use Lookup, Insert, or Delete.");
            }
        }

    // helper to process commands from name server (Entry, Exit)
    private void serverCommand(String message,  Socket socket, bnserver server) {
        String commandParts[] = message.split(" ");
        String command = commandParts[0];
        
        if (command.equalsIgnoreCase("Entry")) {
            System.out.println("Received Entry command from name server.");
            int newNodeId = Integer.parseInt(commandParts[1]);
            int newNodePort = Integer.parseInt(commandParts[2]);
            String traversalList = "";
            if (commandParts.length > 3) {
                traversalList = commandParts[3];
            }
            if (ownId(newNodeId) == true) { // Id in bootServer's range
                String idTravseralListFinal = traversalList.isEmpty() ? String.valueOf(id) : traversalList + "," + id;
                if (predecessorId == id && successorId == id) { // Only 1 node in ring
                        // Send message to new node with succesor/predecessor info
                         // BootServer is only server in ring
                        try {
                            Socket newNodeSocket = new Socket("LocalHost", newNodePort);
                            PrintWriter output = new PrintWriter(newNodeSocket.getOutputStream(), true);
                            
                            String entryRespondMessage = "ENTRY_OK " + id + " " + port + " " + id + " " + port + " " + idTravseralListFinal;
                            // Message: "Entry_OK <id> <port> <successorId> <successorPort>"
                            output.println(entryRespondMessage);
                            // Update successor/predecessor info
                            successorId = newNodeId;
                            successorPort = newNodePort;
                            predecessorId = newNodeId;
                            predecessorPort = newNodePort;
                            newNodeSocket.close();
                            System.out.println(newNodeSocket.isClosed());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                } else {
                    // Case when entering server's ID is in the bootserver's range.
                    try {

                        Socket newNodeSocket = new Socket("LocalHost", newNodePort);
                        PrintWriter output = new PrintWriter(newNodeSocket.getOutputStream(), true);

                        /*
                        SortedMap<Integer, String> keysToTransfer = server.localKeyData.subMap(server.predecessorId + 1, newNodeId + 1);
                        List<Integer> keys = new ArrayList<>(keysToTransfer.keySet());

                        output.println("KeyTransferStart");
                        for (Integer key : keys) {
                            String value = server.localKeyData.get(key);
                            output.println("KeyTransfer: " + key + " " + value);
                            server.localKeyData.remove(key);
                        }
                        output.println("KeyTransferEnd");
                        */

                        String entryMessage = "ENTRY_OK " + id + " " + port + " " + predecessorId + " " + predecessorPort + " " + idTravseralListFinal;
                        //Message: "ENTRY_OK <succesorId> <successorPort> <predecessorId> <predecessorPort>"
                        output.println(entryMessage);


                        // Send message to predecessor to update its info
                        Socket oldPredecessorSocket = new Socket("LocalHost", predecessorPort);
                        PrintWriter oldPredecessorOutput = new PrintWriter(oldPredecessorSocket.getOutputStream(), true);
                        String updatePredecessorMessage = "update_successor " + newNodeId + " " + newNodePort;
                        oldPredecessorOutput.println(updatePredecessorMessage);

                        // Update predecessor info
                        predecessorId = newNodeId;
                        predecessorPort = newNodePort;
                        System.out.println(newNodeSocket.isClosed() + "range in boot");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                // Forward message to succesor
                try {
                    Socket successorSocket = new Socket("LocalHost", successorPort);
                    PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);

                    //If the traversal list is empty, add the node id. If it's not empty, append id onto it. Separated by commas to avoid " " splitting
                    String updatedTraversalList = traversalList.isEmpty() ? String.valueOf(id) : traversalList + "," + id;
                    String messageToForward = "Entry " + newNodeId + " " + newNodePort + " " + updatedTraversalList;
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
            try {
                Socket successorSocket = new Socket("LocalHost", successorPort);
                PrintWriter output = new PrintWriter(successorSocket.getOutputStream(), true);
                output.println("Sending_data");
                for (Map.Entry<Integer, String> entry : localKeyData.entrySet()) {
                System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
                output.println(entry.getKey() + entry.getValue());
                    // Key , Value
                }
                output.println("End_data");
                successorSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            

        } else if (command.equalsIgnoreCase("update_successor")) {
            int newSuccessorId = Integer.parseInt(commandParts[1]);
            int newSuccessorPort = Integer.parseInt(commandParts[2]);
            successorId = newSuccessorId;
            successorPort = newSuccessorPort;
            System.out.println("Updated successor to: " + successorId);
            
        } else if (command.equalsIgnoreCase("Exit")) {

        } else {
            System.out.println("Invalid command from name server. Please use Entry or Exit.");
        }


         // Process the message/command from the name server



    }

    // helper to receive entry message from name server
    private void entry(Socket socket) {
          

    }

    private void insert(int key, String value) {

    }

    private void delete(int key) {

    }

    // check if Id is in BootServers'range
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


