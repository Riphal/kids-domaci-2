package servent.message.snapshot;

import app.AppConfig;
import app.ServentInfo;
import servent.message.BasicMessage;
import servent.message.Message;
import servent.message.MessageType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class AcharyaBadrinathTellAmountMessage extends BasicMessage {

    private static final long serialVersionUID = -643618544187514941L;

    private final List<Message> sendTransactions;
    private final List<Message> receivedTransactions;

    public AcharyaBadrinathTellAmountMessage(ServentInfo sender, ServentInfo receiver, ServentInfo neighbor,
                                             Map<Integer, Integer> senderVectorClock, int amount,
                                             List<Message> sendTransactions, List<Message> receivedTransactions) {
        super(MessageType.ACHARYA_BADRINATH_TELL_AMOUNT, sender, receiver, neighbor, senderVectorClock, String.valueOf(amount));

        this.sendTransactions = new CopyOnWriteArrayList<>(sendTransactions);
        this.receivedTransactions = new CopyOnWriteArrayList<>(receivedTransactions);
    }

    protected AcharyaBadrinathTellAmountMessage(MessageType type, ServentInfo originalSenderInfo, ServentInfo originalReceiverInfo,
                           ServentInfo receiverInfo, Map<Integer, Integer> senderVectorClock,
                           List<ServentInfo> routeList, String messageText, int messageId,
                            List<Message> sendTransactions, List<Message> receivedTransactions) {

        super(type, originalSenderInfo, originalReceiverInfo, receiverInfo, senderVectorClock, routeList, messageText, messageId);

        this.sendTransactions = sendTransactions;
        this.receivedTransactions = receivedTransactions;
    }

    public List<Message> getSendTransactions() {
        return sendTransactions;
    }

    public List<Message> getReceivedTransactions() {
        return receivedTransactions;
    }

    /**
     * Used when resending a message. It will not change the original owner
     * (so equality is not affected), but will add us to the route list, so
     * message path can be retraced later.
     */
    @Override
    public Message makeMeASender() {
        ServentInfo newRouteItem = AppConfig.myServentInfo;

        List<ServentInfo> newRouteList = new ArrayList<>(getRoute());
        newRouteList.add(newRouteItem);
        Message toReturn = new AcharyaBadrinathTellAmountMessage(getMessageType(),
                getOriginalSenderInfo(), getOriginalReceiverInfo(),
                getReceiverInfo(), getSenderVectorClock(),
                newRouteList, getMessageText(), getMessageId(),
                getSendTransactions(), getReceivedTransactions()
        );

        return toReturn;
    }

    /**
     * Change the message received based on ID. The receiver has to be our neighbor.
     * Use this when you want to send a message to multiple neighbors, or when resending.
     */
    @Override
    public Message changeReceiver(Integer newReceiverId) {
        if (AppConfig.myServentInfo.getNeighbors().contains(newReceiverId)) {
            ServentInfo newReceiverInfo = AppConfig.getInfoById(newReceiverId);

            Message toReturn = new AcharyaBadrinathTellAmountMessage(getMessageType(),
                    getOriginalSenderInfo(), getOriginalReceiverInfo(),
                    newReceiverInfo, getSenderVectorClock(),
                    getRoute(), getMessageText(), getMessageId(),
                    getSendTransactions(), getReceivedTransactions()
            );

            return toReturn;
        } else {
            AppConfig.timestampedErrorPrint("Trying to make a message for " + newReceiverId + " who is not a neighbor.");

            return null;
        }

    }
}
