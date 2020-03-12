package cs245.as3;

import cs245.as3.interfaces.StorageManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class Transaction {
  long _txnid;
  //List<LogMsg> _orderedLogMessages;
  //List<TransactionManager.WritesetEntry> _orderedWriteSets;
  private HashMap<Long, StorageManager.TaggedValue> _latestValues;
  boolean _isCommitted;
  //boolean _isCompacted;
  boolean _isAborted;

  public Transaction(LogMsg logmsg) {
    if (!logmsg.isStartLog()) {
      throw new RuntimeException("Txn must always be constructed using a start message");
    }
    this._txnid = logmsg.getTxnid();
    //this._orderedLogMessages = new ArrayList<>();
    //this._orderedLogMessages.add(logmsg); //start msg
    //this._orderedWriteSets = new ArrayList<>();
    _isCommitted = false;
    //_isCompacted = false;
    _isAborted = false;
    _latestValues = new HashMap<>();
  }

  public void writeLogEncountered(LogMsg logmsg) {
    if (!logmsg.isWriteLog()) {
      throw new RuntimeException("Not a write message");
    }
    if (logmsg.getTxnid() != this._txnid) {
      throw new RuntimeException("Not belonging to this txn");
    }
    //_orderedLogMessages.add(logmsg);
    //TransactionManager.WritesetEntry wse = new TransactionManager.WritesetEntry(logmsg.getKey(), logmsg.getVal());
    //_orderedWriteSets.add(wse);
    int tag = 0;
    this._latestValues.put(logmsg.getKey(), new StorageManager.TaggedValue(tag, logmsg.getVal()));
  }

  public void commitLogEncountered(LogMsg logmsg) {
    if (!logmsg.isCommitLog()) {
      throw new RuntimeException("Not a commit message");
    }
    if (logmsg.getTxnid() != this._txnid) {
      throw new RuntimeException("Not belonging to this txn");
    }
    if(isAborted()){
      throw new RuntimeException("Can't commit a aborted txn"+getTxnid());
    }
    //_orderedLogMessages.add(logmsg);
    _isCommitted = true;
  }

  public boolean isAborted() {
    return _isAborted;
  }

  public void abortLogEncountered(LogMsg logmsg) {
    if (!logmsg.isAbortLog()) {
      throw new RuntimeException("Not a abort message");
    }
    if (logmsg.getTxnid() != this._txnid) {
      throw new RuntimeException("Not belonging to this txn");
    }
    if(isCommitted()){
      throw new RuntimeException("Can't abort a committed txn"+getTxnid());
    }
    //_orderedLogMessages.add(logmsg);
    _isAborted = true;
  }

  /*public boolean isCompacted() {
    return _isCompacted;
  }*/

  /*public void compactThisTxnToCreateLVmap(){
    if(!this.isCommitted()) throw new RuntimeException("Cannot compact txn log without commit");
    for(TransactionManager.WritesetEntry x : getOrderedWriteSets()) {
      //tag is unused in this implementation:
      long tag = 0;
      this._latestValues.put(x.key, new StorageManager.TaggedValue(tag, x.value));
    }
    _isCompacted = true;
  }*/

  public long getTxnid() {
    return _txnid;
  }

  /*public List<LogMsg> getOrderedLogMessages() {
    return _orderedLogMessages;
  }*/

  public boolean isCommitted() {
    return _isCommitted;
  }

  /*private List<TransactionManager.WritesetEntry> getOrderedWriteSets() {
    return _orderedWriteSets;
  }*/

  public HashMap<Long, StorageManager.TaggedValue> getLatestValues() {
    //if(!isCompacted()) throw new RuntimeException("Please compact Txn log before asking for latestValues");
    return _latestValues;
  }
}