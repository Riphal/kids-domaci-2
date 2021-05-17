package servent.handler;

import app.AppConfig;
import app.CausalBroadcastShared;
import app.ServentInfo;
import app.snapshot_bitcake.SnapshotCollector;
import servent.message.Message;
import servent.message.util.MessageUtil;

import java.util.Set;

public class CausalBrodcastHandler implements MessageHandler {
    private final Message clientMessage;
    private final SnapshotCollector snapshotCollector;
    private final Set<Message> receivedBroadcasts;
    private final Object pendingMessagesLock;

    public CausalBrodcastHandler(Message clientMessage, SnapshotCollector snapshotCollector,
                                 Set<Message> receivedBroadcasts, Object pendingMessagesLock) {
        this.clientMessage = clientMessage;
        this.snapshotCollector = snapshotCollector;
        this.receivedBroadcasts = receivedBroadcasts;
        this.pendingMessagesLock = pendingMessagesLock;
    }

    @Override
    public void run() {
        ServentInfo senderInfo = clientMessage.getOriginalSenderInfo();
        ServentInfo lastSenderInfo = clientMessage.getRoute().size() == 0 ?
                clientMessage.getOriginalSenderInfo() :
                clientMessage.getRoute().get(clientMessage.getRoute().size() - 1);

        /*
         * The standard read message already prints out that we got a msg.
         * However, we also want to see who sent this to us directly, besides from
         * seeing the original owner - if we are not in a clique, this might
         * not be the same node.
         */
        String text = String.format("Got %s from %s broadcast by %s",
                clientMessage.getMessageText(), lastSenderInfo, senderInfo);

//        AppConfig.timestampedStandardPrint(text);

        if (senderInfo.getId() == AppConfig.myServentInfo.getId()) {
            //We are the sender :o someone bounced this back to us. /ignore
            AppConfig.timestampedStandardPrint("Got own message back. No rebroadcast.");
        } else {
            synchronized (pendingMessagesLock) {
                //Try to put in the set. Thread safe add ftw.
                boolean didPut = receivedBroadcasts.add(clientMessage);

                if (didPut) {
                    CausalBroadcastShared.addPendingMessage(clientMessage);
                    CausalBroadcastShared.checkPendingMessages();

                    if (!AppConfig.IS_CLIQUE) {
                        //New message for us. Rebroadcast it.
                        AppConfig.timestampedStandardPrint("Rebroadcasting... " + receivedBroadcasts.size());

                        for (Integer neighbor : AppConfig.myServentInfo.getNeighbors()) {
                            //Same message, different receiver, and add us to the route table.
                            MessageUtil.sendMessage(clientMessage.changeReceiver(neighbor).makeMeASender());
                        }
                    }
                } else {
                    //We already got this from somewhere else. /ignore
                    AppConfig.timestampedStandardPrint("Already had this. No rebroadcast.");
                }
            }
        }
    }
}
