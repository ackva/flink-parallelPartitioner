package org.myorg.quickstart.sharedState;

// A sample Java program to demonstrate HashMap and HashTable
import java.util.*;
import java.lang.*;
import java.io.*;


/* Name of the class has to be "Main" only if the class is public. */
public class TestHashMap {

    public static void main(String args[])
    {
        //----------hashtable -------------------------
        Hashtable<Integer,String> ht=new Hashtable<Integer,String>();
        ht.put(1,"1");
        ht.put(2,"2");  // hash map allows duplicate values
        ht.put(3,"3");  // hash map allows duplicate values
        ht.put(4,"4");
        ht.put(4,"5");
        System.out.println("-------------Hash table--------------");
        for (Map.Entry m:ht.entrySet()) {
            System.out.println(m.getKey()+" "+m.getValue());
        }

        //----------------hashmap--------------------------------
        HashMap<Integer,String> hm=new HashMap<Integer,String>();
        hm.put(1,"2");
        if(!hm.containsKey(1))
            hm.put(1,"0");
        else {
            String temp = hm.get(1);
            hm.put(1,temp + ";" + 0);
        }
        hm.put(2,"2");  // hash map allows duplicate values
        hm.put(3,"3");  // hash map allows duplicate values
        hm.put(4,"4");
        hm.put(4,"5");
        System.out.println("-----------Hash map-----------");
        for (Map.Entry m:hm.entrySet()) {
            System.out.println(m.getKey()+" "+m.getValue());
        }
    }
}