package cs245.as3;

import cs245.as3.interfaces.StorageManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;


public class Transaction {
  long _txnid;
  private List<StorageManager.TaggedValue> _latestValues;
  boolean _isCommitted;
  boolean _isAborted;

  int _begin;
  Set<Integer> kvhashTODO;
  boolean flushedToDisk;

  public Transaction(LogMsg logmsg, int startLogIdx) {
    this._txnid = logmsg.getTxnid();
    _isCommitted = false;
    _isAborted = false;
    _latestValues = new ArrayList<>();
    kvhashTODO = new HashSet<>();
    _begin = startLogIdx;
  }

  public Transaction(long txnid, int startLogIdx) {
    this._txnid = txnid;
    _isCommitted = false;
    _isAborted = false;
    _latestValues = new ArrayList<>();
    kvhashTODO = new HashSet<>();
    _begin = startLogIdx;
  }

  public void flush(long key, byte[] val){
    kvhashTODO.remove(Objects.hash(key, val));
    flushedToDisk = kvhashTODO.isEmpty();
  }

  public void writeLogEncountered(LogMsg logmsg) {
    /*if (!logmsg.isWriteLog()) {
      throw new RuntimeException("Not a write message");
    }
    if (logmsg.getTxnid() != this._txnid) {
      throw new RuntimeException("Not belonging to this txn");
    }
*/
    long tag = logmsg.getKey(); //_txnId ?? TODO
    this._latestValues.add(new StorageManager.TaggedValue(tag, logmsg.getVal()));
    kvhashTODO.add(Objects.hash(logmsg.getKey(), logmsg.getVal()));
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
    _isAborted = true;
  }

  public long getTxnid() {
    return _txnid;
  }


  public boolean isCommitted() {
    return _isCommitted;
  }


  public List<StorageManager.TaggedValue> getLatestValues() {
    return _latestValues;
  }

  public int getBegin() {
    return _begin;
  }
}