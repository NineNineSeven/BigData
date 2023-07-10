package mp.hadoop.project.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NameParser implements WritableComparable<NameParser> {
    private String name;
    private float chance;

    public NameParser(){
        name = "";
        chance = 0;
    }

    public NameParser(String name,float chance){
        this.name = name;
        this.chance = chance;
    }

    public String getName(){
        return this.name;
    }

    public float getChance(){
        return this.chance;
    }

    public void setChance(float chance){this.chance = chance;}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeFloat(chance);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        chance = in.readFloat();
    }

    @Override
    public String toString(){
        return name + "," + chance;
    }

    @Override
    public int compareTo(NameParser o) {
        return this.chance < o.getChance()? -1:1;
    }
}