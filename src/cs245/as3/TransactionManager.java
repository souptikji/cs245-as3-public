package cs245.as3;

import java.util.ArrayList;
import java.util.HashMap;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 * Heap dump testing : java -Xmx1400m -Xms1400m -XX:+HeapDumpOnOutOfMemoryError -jar target/tests.jar
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
	static class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}
	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	  */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;

	private StorageManager _sm;
	private LogManager _lm;

	public TransactionManager() {
		writesets = new HashMap<>();
		//see initAndRecover
		latestValues = null;
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		latestValues = sm.readStoredTable();
		this._sm = sm;
		this._lm = lm;
		wakeUpFromCrash();
	}

	private void wakeUpFromCrash() {
		Map<Long, Transaction> allTxnsMap = TransactionUtils.deserializeEntireLog(this._lm);
		List<Transaction> committedTxns = allTxnsMap.values().stream().filter(txn -> txn.isCommitted()).collect(Collectors.toList());
		committedTxns.sort((Transaction a, Transaction b)-> Long.compare(a.getTxnid(), b.getTxnid()));

		//committedTxns.forEach(txn -> txn.compactThisTxnToCreateLVmap());
		committedTxns.forEach(txn -> latestValues.putAll(txn.getLatestValues()));

		//Queue all committed writes to disk
		for(long key: latestValues.keySet()){
			//Start pushing to dick
			TaggedValue tv = latestValues.get(key);
			_sm.queueWrite(key, tv.tag, tv.value);
		}

		//TODO: "End" txn
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txnid) {
		//LogMsg startLog = new LogMsg((byte) 1, txnid);
		//_lm.appendLogRecord(startLog.serialize());
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read()
	 * calls until the transaction making the write commits. For simplicity, we will not make reads
	 * to this same key from txID itself after we make a write to the key.
	 */
	public void write(long txID, long key, byte[] value) {
		//LogMsg writeLog = new LogMsg((byte) 2, txID, key, value);
		//_lm.appendLogRecord(writeLog.serialize());

		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset == null) {
			writeset = new ArrayList<>();
			writesets.put(txID, writeset);
		}
		writeset.add(new WritesetEntry(key, value));
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		Map<Long, TaggedValue> pushTheseToDisk = new HashMap<>();

		// Modify in memory data structure
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
				//tag is unused in this implementation:
				long tag = 0; //TODO: Change this
				TaggedValue tv = new TaggedValue(tag, x.value);
				latestValues.put(x.key, tv);
				pushTheseToDisk.put(x.key, tv);
			}
			writesets.remove(txID);
		}

		// Start logging to disk
		//LogMsg startLog = new LogMsg((byte) 1, txID);
		//_lm.appendLogRecord(startLog.serialize());
		for(long key: pushTheseToDisk.keySet()){
			TaggedValue tv = pushTheseToDisk.get(key);
			LogMsg writeLog = new LogMsg((byte) 2, txID, key, tv.value);
			_lm.appendLogRecord(writeLog.serialize());
		}
		LogMsg commitLog = new LogMsg((byte) 3, txID);
		_lm.appendLogRecord(commitLog.serialize());

		// Start writing to disk
		for(long key: pushTheseToDisk.keySet()){
			TaggedValue tv = pushTheseToDisk.get(key);
			_sm.queueWrite(key, tv.tag, tv.value);
		}
	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		//LogMsg abortLog = new LogMsg((byte) 4, txID);
		//_lm.appendLogRecord(abortLog.serialize());
		writesets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
	}
}
