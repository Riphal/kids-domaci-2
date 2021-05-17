package app.snapshot_bitcake;

import servent.message.Message;

import java.util.List;

/**
 * This class is used if the user hasn't specified a snapshot type in config.
 * 
 * @author bmilojkovic
 *
 */
public class NullSnapshotCollector implements SnapshotCollector {

	@Override
	public void run() {}

	@Override
	public void stop() {}

	@Override
	public BitcakeManager getBitcakeManager() {
		return null;
	}

	@Override
	public void addNaiveSnapshotInfo(String snapshotSubject, int amount) {}

	@Override
	public void addAcharyaBadrinathSnapshotInfo(String snapshotSubject, int amount,
				List<Message> sendTransactions, List<Message> receivedTransactions) {}

	@Override
	public void startCollecting() {}

}
