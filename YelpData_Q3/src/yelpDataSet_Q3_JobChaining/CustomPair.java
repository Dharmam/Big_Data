package yelpDataSet_Q3_JobChaining;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class CustomPair implements WritableComparable<CustomPair>{  
	// Data members
	private Text data;
	private Text identifier;
	
	public  CustomPair(){
	    this.data=new Text();
	    this.identifier=new Text();
	}
	
	public CustomPair(Text data, Text identifier) {
	    //super();
	    this.data = data;
	    this.identifier = identifier;
	}
	public CustomPair(String data,String identifier){
	    this.data=new Text(data);
	    this.identifier=new Text(identifier);
	}
	
	public Text getdata() {
	    return data;
	}
	
	public void setdata(Text data) {
	    this.data = data;
	}
	
	public Text getidentifier() {
	    return identifier;
	}
	
	public void setidentifier(Text identifier) {
	    this.identifier = identifier;
	}
	public void set(Text data,Text identifier){
	    this.data=data;
	    this.identifier=identifier;
	}
	
	@Override
	public int hashCode() {
	    // TODO Auto-generated method stub
	    return data.hashCode()*163+identifier.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
	    // TODO Auto-generated method stub
	    if(obj instanceof CustomPair){
	    	CustomPair tp=(CustomPair)obj;
	        return data.equals(tp.getdata())&&identifier.equals(tp.getidentifier());
	    }
	    return false;
	}
	
	@Override
	public String toString() {
	    // TODO Auto-generated method stub
	    return data+"\t"+identifier;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
	    // TODO Auto-generated method stub
	    data.readFields(in);
	    identifier.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
	    // TODO Auto-generated method stub
	    data.write(out);
	    identifier.write(out);
	}
	
	
	
	
	@Override
	public int compareTo(CustomPair tp) {
	    // TODO Auto-generated method stub
	    int cmp=data.compareTo(tp.getdata());
	    if(cmp!=0)
	        return cmp;
	    return identifier.compareTo(tp.getidentifier());
	    
	}
	
	public CustomPair reverse() {
		return new CustomPair(identifier,data);
	}
}