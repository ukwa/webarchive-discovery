package uk.bl.wap.hadoop;

/**
 * 
 * @author Andrew.Jackson@bl.uk
 */
public class MutableInt {
	int value = 0;
	public void inc () { ++value; }
	public int get () { return value; }
	
	@Override
	public String toString() {
		return Integer.toString(value);
	}
	  
}