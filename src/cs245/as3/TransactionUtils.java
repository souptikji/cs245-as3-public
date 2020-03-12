package cs245.as3;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;


public class TransactionUtils {

  /*public static SortedMap<Long, Transaction> deserializeEntireLog(LogManager lm) {
    SortedMap<Long, Transaction> map = new TreeMap<>();
    int rawlogsize = lm.getLogEndOffset();
    if (rawlogsize % 128 != 0) {
      throw new RuntimeException("Log should always be multiples of 128");
    }
    int loglen = rawlogsize / 128;

    // First pass
    Set<Long> committedTxnIds = new HashSet<>();
    for (int i = 0; i < loglen; ++i) {
      byte[] logmsgArr = lm.readLogRecord(i * 128, 128);
      LogMsg log = LogMsg.deserialize(logmsgArr);
      if (log.isCommitLog()) {
        committedTxnIds.add(log.getTxnid());
      }
    }

    // Second pass
    for (int i = 0; i < loglen; ++i) {
      byte[] logmsgArr = lm.readLogRecord(i * 128, 128);
      LogMsg log = LogMsg.deserialize(logmsgArr);
      if(!committedTxnIds.contains(log.getTxnid())) continue;
      if (log.isStartLog()) {
        Transaction txn = new Transaction(log);
        map.put(txn._txnid, txn);
      } else if (log.isWriteLog()) {
        // Verify it has a start
        Transaction txn = map.get(log.getTxnid());
        if (txn == null) {
          throw new RuntimeException("Txn " + txn + " has a write message but no corresponding start");
        }
        txn.writeLogEncountered(log);
      } else if (log.isCommitLog()) {
        Transaction txn = map.get(log.getTxnid());
        if (txn == null) {
          throw new RuntimeException("Txn " + txn + " has a commit message but no corresponding start");
        }
        txn.commitLogEncountered(log);
      }
    } //for
    return map;
  }*/

  public static void deserializeEntireLog2(LogManager lm, HashMap<Long, StorageManager.TaggedValue> latestValues) {
    int rawlogsize = lm.getLogEndOffset();
    if (rawlogsize % 128 != 0) {
      throw new RuntimeException("Log should always be multiples of 128");
    }
    int loglen = rawlogsize / 128;

    // First pass
    Set<Long> committedTxnIds = new HashSet<>();
    for (int i = 0; i < loglen; ++i) {
      byte[] logmsgArr = lm.readLogRecord(i * 128, 128);
      LogMsg log = LogMsg.deserialize(logmsgArr);
      if (log.isCommitLog()) {
        committedTxnIds.add(log.getTxnid());
      }
    }

    // Second pass
    for (int i = 0; i < loglen; ++i) {
      byte[] logmsgArr = lm.readLogRecord(i * 128, 128);
      LogMsg log = LogMsg.deserialize(logmsgArr);
      if(!committedTxnIds.contains(log.getTxnid())) continue;
      if (log.isWriteLog()) {
        int tag = 0;
        latestValues.put(log.getKey(), new StorageManager.TaggedValue(tag, log.getVal()));
      }
    } //for
    return;
  }
}
