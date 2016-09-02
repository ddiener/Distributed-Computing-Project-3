import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Scanner;

/**
 * The Coordinator spawns threads to deal with clients, while keeping track of
 * the multicast group. The Coordinator also handles the multicasting of messages
 * to reconnecting clients, subject to the time constraint.
 */
public class Coordinator {
    private static ArrayList<HandlerThread> participants = new ArrayList<>();
    private static ArrayList<Pair<String, Long>> messageLog = new ArrayList<>();
    private static Long tD; // in seconds

    /**
     * Send the message to the multicast group.
     * Save the message in the message queue.
     * @param message the alphanumeric string to be sent.
     * @throws IOException
     */
    public static void multicast(String message) throws IOException {
        Long timestamp = System.currentTimeMillis() / 1000;
        Pair<String, Long> timestampedMsg = new Pair<>(message, timestamp);
        messageLog.add(timestampedMsg);
        for (HandlerThread p : participants) {
            p.sendMessage(message);
        }
    }

    /**
     * Reconnect the specified thread and send any message that satisfies the
     * time constraint.
     * @param recThread the thread which is to be reconnected.
     * @throws IOException
     */
    public static void reconnect(HandlerThread recThread) throws IOException {
        if (!participants.contains(recThread))
            // The thread is not a participant, abort
            return;

        long currentTime = System.currentTimeMillis() / 1000;
        for (Iterator<Pair<String, Long>> iterator = messageLog.iterator(); iterator.hasNext();) {
            Pair<String, Long> p = iterator.next();
            // Check to see if this pair satisfies the time constraint
            if (currentTime - p.getRight() > tD) {
//                messageLog.remove(p); // This line causes a concurrency issue
                iterator.remove();
            }
            else {
                recThread.sendMessage(p.getLeft());
            }
        }
    }

    /**
     * Remove the specified thread from the multicast group.
     * @param deregThread thread to be removed from multicast group.
     */
    public static void deregister(HandlerThread deregThread) {
        if (participants.contains(deregThread))
            participants.remove(deregThread);
    }

    /**
     * Register the specified thread in the multicast group.
     * @param regThread thread to be added to the multicast group.
     */
    public static void register(HandlerThread regThread) {
        if (!participants.contains(regThread))
            participants.add(regThread);
    }

    /**
     * Listen on the specified port and spawn off handler threads.
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: java Coordinator [config file name]");
            return;
        }

        // takes in configuration file from the command line
        String configFilename = args[0];
        Scanner configFile = new Scanner(new File(configFilename));

        // parsed info from the given configuration file
        String portNumber = configFile.nextLine();
        int port = Integer.parseInt(portNumber);

        tD = configFile.nextLong();

        ServerSocket server = new ServerSocket(port);

        while(true) {
            Socket participant = server.accept();
            HandlerThread handlerThread = new HandlerThread(participant);
            handlerThread.start();
        }
    }
}

/**
 * HandlerThread handles I/O with an individual client.
 */
class HandlerThread extends Thread {
    private Socket threadA;
    private PrintWriter threadAW;
    private BufferedReader threadAR;
    private Socket threadB;
    private PrintWriter threadBW;
    private BufferedReader threadBR;
    private String participantId;

    /**
     * Create a HandlerThread.
     * @param participant the socket representing the participant.
     * @throws IOException
     */
    public HandlerThread(Socket participant) throws IOException {
        threadA = participant;
        threadAW = new PrintWriter(threadA.getOutputStream(), true);
        threadAR = new BufferedReader(new InputStreamReader(threadA.getInputStream()));
    }

    /**
     * Get and interpret commands from the participant.
     */
    public void run() {
        try {
            String line;
            String[] splitLine;
            while (true) {
                line = threadAR.readLine();
                splitLine = line.split(" ");

                /*
                Participant has to register with the coordinator specifying its
                ID, IP address and port number where its thread-B will receive
                multicast messages (thread-B has to be operational before sending
                the message to the coordinator). Upon successful registration,
                the threadA is a member of the multicast group and will begin
                receiving messages.
                 */
                if (splitLine[0].equals("register")) {
                    Coordinator.register(this);
                    participantId = splitLine[1];
                    String participantIP = splitLine[2];
                    int participantPort = Integer.parseInt(splitLine[3]);
                    threadB = new Socket(participantIP, participantPort);
                    threadBW = new PrintWriter(threadB.getOutputStream(), true);
                    threadBR = new BufferedReader(new InputStreamReader(threadB.getInputStream()));
                    threadAW.println();
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
                    Coordinator.deregister(this);
                }
                /*
                Participant indicates to the coordinator that it is temporarily
                going offline. The coordinator will have to send it messages
                sent during disconnection (subject to temporal constraint).
                Thread-B will relinquish the port and may become dormant or die.
                 */
                else if (splitLine[0].equals("disconnect")) {
                    Coordinator.deregister(this);
                }
                /*
                Participant indicates to the coordinator that it is online and
                it will specify the IP address and port number where its thread-B
                will receive multicast messages (thread-B has to be operational
                before sending the message to the coordinator).
                 */
                else if (splitLine[0].equals("reconnect")) {
                    Coordinator.register(this);
                    participantId = splitLine[1];
                    String participantIP = splitLine[2];
                    int participantPort = Integer.parseInt(splitLine[3]);
                    threadB = new Socket(participantIP, participantPort);
                    threadBW = new PrintWriter(threadB.getOutputStream(), true);
                    threadBR = new BufferedReader(new InputStreamReader(threadB.getInputStream()));
                    threadAW.println();

                    Coordinator.reconnect(this);
                }
                /*
                Multicast [message] to all current members. Note that [message]
                is an alpha-numeric string (e.g., UGACSRocks). The particpant
                sends the message to the coordinator and unblocks after an
                acknowledgement is received.
                 */
                else if (splitLine[0].equals("msend")) {
                    DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
                    Date date = new Date();
                    String message = "(" + dateFormat.format(date) + ") ";
                    message += participantId + ": ";
                    for (int i = 1; i < splitLine.length; i++)
                        message += splitLine[i] + " ";
                    Coordinator.multicast(message);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send a message to the participant's listening thread.
     * @param message the alphanumeric message to be sent.
     * @throws IOException
     */
    public void sendMessage(String message) throws IOException {
        threadBW.println(message);
    }
}

/**
 * Pair represents a tuple of type L and type R.
 * http://stackoverflow.com/questions/156275/what-is-the-equivalent-of-the-c-pairl-r-in-java/3646398#3646398
 * @param <L> the left side of the tuple.
 * @param <R> the right side of the tuple.
 */
class Pair<L,R> {
    private final L left;
    private final R right;

    /**
     * Standard pair constructor.
     * @param left left side of the tuple.
     * @param right right side of the tuple.
     */
    public Pair(L left, R right) {
        this.left = left;
        this.right = right;
    }

    /**
     * Get the left part of the tuple.
     */
    public L getLeft() { return left; }

    /**
     * Get the right part of the tuple.
     */
    public R getRight() { return right; }

    /**
     * Get the left part of the tuple.
     */
    @Override
    public int hashCode() { return left.hashCode() ^ right.hashCode(); }

    /**
     * Test whether one pair is equal to another.
     * @param o object to be compared.
     * @return true if equal, false if not.
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Pair)) return false;
        Pair pairo = (Pair) o;
        return this.left.equals(pairo.getLeft()) &&
                this.right.equals(pairo.getRight());
    }
}