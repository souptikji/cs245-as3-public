package cs245.as3;

import java.util.ArrayList;
import java.util.List;


public class Transaction {
  long _txnid;
  List<LogMsg> _orderedLogMessages;
  List<TransactionManager.WritesetEntry> _orderedWriteSets;
  boolean _isCommitted;

  public Transaction(LogMsg logmsg) {
    if (!logmsg.isStartLog()) {
      throw new RuntimeException("Txn must always be constructed using a start message");
    }
    this._txnid = logmsg.getTxnid();
    this._orderedLogMessages = new ArrayList<>();
    this._orderedLogMessages.add(logmsg); //start msg
    this._orderedWriteSets = new ArrayList<>();
    _isCommitted = false;
  }

  public void writeLogEncountered(LogMsg logmsg) {
    if (!logmsg.isWriteLog()) {
      throw new RuntimeException("Not a write message");
    }
    if (logmsg.getTxnid() != this._txnid) {
      throw new RuntimeException("Not belonging to this txn");
    }
    _orderedLogMessages.add(logmsg);
    TransactionManager.WritesetEntry wse = new TransactionManager.WritesetEntry(logmsg.getKey(), logmsg.getVal());
    _orderedWriteSets.add(wse);
  }

  public void commitLogEncountered(LogMsg logmsg) {
    if (!logmsg.isCommitLog()) {
      throw new RuntimeException("Not a commit message");
    }
    if (logmsg.getTxnid() != this._txnid) {
      throw new RuntimeException("Not belonging to this txn");
    }
    _orderedLogMessages.add(logmsg);
    _isCommitted = true;
  }

  public long getTxnid() {
    return _txnid;
  }

  public List<LogMsg> getOrderedLogMessages() {
    return _orderedLogMessages;
  }

  public boolean isCommitted() {
    return _isCommitted;
  }
}
