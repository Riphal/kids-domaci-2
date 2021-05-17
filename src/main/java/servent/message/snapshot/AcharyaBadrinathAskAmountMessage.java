package servent.message.snapshot;

import app.ServentInfo;
import servent.message.BasicMessage;
import servent.message.MessageType;

import java.util.Map;

public class AcharyaBadrinathAskAmountMessage extends BasicMessage {

    private static final long serialVersionUID = -1189395068257017543L;

    public AcharyaBadrinathAskAmountMessage(ServentInfo sender, ServentInfo receiver, ServentInfo neighbor,
                                            Map<Integer, Integer> senderVectorClock) {
        super(MessageType.ACHARYA_BADRINATH_ASK_AMOUNT, sender, receiver, neighbor, senderVectorClock);
    }
}
