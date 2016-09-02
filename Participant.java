import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

/**
 * ThreadA takes input from the user and relays it to its respective Coordinator
 * thread.
 */
class ThreadA extends Thread {
    private Socket server;
    private ThreadB threadB;

    /**
     * Create a ThreadA object.
     */
    public ThreadA() {
        try {
            server = new Socket(Participant.coordIp, Participant.coordPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Take input from the user.
     */
    public void run() {
        try {
            PrintWriter w = new PrintWriter(server.getOutputStream(), true);
            BufferedReader r = new BufferedReader(new InputStreamReader(server.getInputStream()));
            Scanner input = new Scanner(System.in);
            while (true) {
                System.out.print("participant> ");
                String line = input.nextLine();
                String[] splitLine = line.split(" ");

                /*
                Participant has to register with the coordinator specifying its
                ID, IP address and port number where its thread-B will receive
                multicast messages (thread-B has to be operational before sending
                the message to the coordinator). Upon successful registration,
                the participant is a member of the multicast group and will
                receiving messages.
                 */
                if (splitLine[0].equals("register")) {
                    threadB = new ThreadB(Integer.parseInt(splitLine[1]));
                    threadB.start();
                    line = splitLine[0] + " " + Participant.id + " ";
                    line += InetAddress.getLocalHost().getHostAddress() + " " + splitLine[1];
                    w.println(line);
                    r.readLine();
                }
                /*
                Participant indicates to the coordinator that it is no longer
                belongs to the multicast group. Please note that this is
                different than being disconnected. A participant that deregisters,
                may register again. But it will not get any messages that were
                sent since its deregistration (i.e., it will be treated as a new
                entrant). Thread-B will relinquish the port and may become
                dormant or die.
                 */
                else if (splitLine[0].equals("deregister")) {
                    w.println(line);
                    threadB.interrupt();
//                    threadB.stop(); // The use of this is discouraged
                }
                /*
                Participant indicates to the coordinator that it is temporarily
                going offline. The coordinator will have to send it messages
                sent during disconnection (subject to temporal constraint).
                Thread-B will relinquish the port and may become dormant or die.
                 */
                else if (splitLine[0].equals("disconnect")) {
                    w.println(line);
                    threadB.interrupt();
                }
                /*
                Participant indicates to the coordinator that it is online and
                it will specify the IP address and port number where its thread-B
                will receive multicast messages (thread-B has to be operational
                before sending the message to the coordinator).
                 */
                else if (splitLine[0].equals("reconnect")) {
                    threadB = new ThreadB(Integer.parseInt(splitLine[1]));
                    threadB.start();
                    line = splitLine[0] + " " + Participant.id + " ";
                    line += InetAddress.getLocalHost().getHostAddress() + " " + splitLine[1];
                    w.println(line);
                    r.readLine();
                }
                /*
                Multicast [message] to all current members. Note that [message]
                is an alpha-numeric string (e.g., UGACSRocks). The particpant
                sends the message to the coordinator and unblocks after an
                acknowledgement is received.
                 */
                else if (splitLine[0].equals("msend")) {
                    w.println(line);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/**
 * ThreadB waits on the specified port and receives messages from the Coordinator.
 */
class ThreadB extends Thread {
    private ServerSocket socket;
    private Socket coordinator;
    private FileWriter logFile;

    /**
     * Create a thread which listens on a specifed portnumber.
     * @param portnumber the port where the thread listens.
     * @throws IOException
     */
    public ThreadB(int portnumber) throws IOException {
        socket = new ServerSocket(portnumber);
        logFile = new FileWriter(Participant.logFilename, true);
    }

    /**
     * Write to the logfile, while this thread is not interrupted.
     */
    public void run() {
        try {
            coordinator = socket.accept();
            BufferedReader r = new BufferedReader(new InputStreamReader(coordinator.getInputStream()));

            while (!Thread.currentThread().isInterrupted()) {
                if (r.ready()) {
                    logFile.write(r.readLine() + "\r\n");
                    logFile.flush();
                }
                // TODO this could be consuming a lot of resources, help by including thread sleep call
            }

            // Do cleanup
            socket.close();
            coordinator.close();
            logFile.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/**
 * The Participant handles creates threads that handle I/O with the user.
 */
public class Participant {
    public static String logFilename;
    public static String id;
    public static String coordIp;
    public static int coordPort;

    /**
     * Read from the configuration file and start a I/O thread.
     */
    public static void main(String[] args) throws FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Usage: java Participant [config file]");
            return;
        }
        String configFilename = args[0];
        Scanner configFile = new Scanner(new File(configFilename));

        // parsing strings from the config file
        id = configFile.nextLine();
        logFilename = configFile.nextLine();
        coordIp = configFile.next();

        // parse port number for usable integer
        // TODO should probably be a try catch?
        String portNumber = configFile.next();
        coordPort = Integer.parseInt(portNumber);

        Thread tA = new Thread(new ThreadA());
        tA.start();
    }
}