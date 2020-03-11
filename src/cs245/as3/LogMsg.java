package cs245.as3;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;


public class LogMsg implements Serializable {
  public static final int DONTCAREKEY = -1;

  private final byte _type; //1-start, 2-write, 3-commit, 4-abort
  private final long _txnid;
  private final long _key;
  private final int _valLen;
  private final byte[] _val;

  @Override
  public boolean equals(Object obj) {
    if (!obj.getClass().equals(this.getClass())) {
      System.out.print("Obj class is " + obj.getClass().toString());
      return false;
    }
    LogMsg other = (LogMsg) obj;
    return this._type == other._type && this._txnid == other._txnid && this._key == other._key
        && this._valLen == other._valLen && Arrays.equals(this._val, other._val);
  }

  // Write log msg
  public LogMsg(byte type, long txnid, long key, byte[] val) {
    _type = type;
    _txnid = txnid;
    _key = key;
    _val = val;
    _valLen = val.length;
  }

  // Start, Commit, Abort log message
  public LogMsg(byte type, long txnid) {
    _type = type;
    _txnid = txnid;
    _key = DONTCAREKEY;
    _val = new byte[0];
    _valLen = 0;
  }

  public byte[] serialize() {
    ByteBuffer ret = ByteBuffer.allocate(128);
    ret.put(_type);
    ret.putLong(_txnid);
    ret.putLong(_key);
    ret.putInt(_valLen);
    ret.put(_val);
    return ret.array();
  }

  static LogMsg deserialize(byte[] b) {
    ByteBuffer bb = ByteBuffer.wrap(b);
    long txnid, key;
    int valLen;
    byte[] val;

    byte type = bb.get();
    txnid = bb.getLong();

    if (type == 1) {
      //start txn
      key = DONTCAREKEY;
      valLen = 0;
      val = new byte[0];
    } else if (type == 2) {
      //write
      key = bb.getLong();
      valLen = bb.getInt();
      val = new byte[valLen];
      bb.get(val);
    } else if (type == 3) {
      //commit txn
      key = DONTCAREKEY;
      valLen = 0;
      val = new byte[0];
    } else if (type == 4) {
      // abort txn
      key = DONTCAREKEY;
      valLen = 0;
      val = new byte[0];
    } else {
      //invalid
      throw new RuntimeException("Invalid log type=" + type);
    }
    return new LogMsg(type, txnid, key, val);
  }

  public boolean isStartLog() {
    return this._type == 1;
  }

  public boolean isWriteLog() {
    return this._type == 2;
  }

  public boolean isCommitLog() {
    return this._type == 3;
  }

  public boolean isAbortLog() {
    return this._type == 4;
  }

  public byte getType() {
    return _type;
  }

  public long getTxnid() {
    return _txnid;
  }

  public long getKey() {
    return _key;
  }

  public byte[] getVal() {
    return _val;
  }
}
